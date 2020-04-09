# CS3223 Project
Team Members:
- Arielyte Tsen Chung Ming
- Prem Rajeshkumar Bagdawala

# Table of Contents
1. [Operators](#operators)
    1. [BlockNestedJoin](#block-nested-loop-join)
    1. [Distinct](#distinct)
    1. [GroupBy](#group-by)
    1. [HashJoin](#hash-join)
    1. [PartitionScan](#partition-scan)
    1. [TupleIterator](#tuple-iterator)
1. [Optimizers](#optmizers)
    1. [BufferManager](#buffer-manager)
    1. [PlanCost](#plan-cost)
    1. [RandomInitialPlan](#random-initial-plan)
    1. [RandomOptimizer](#random-optimizer)
1. [Utils](#utils)
    1. [AttrComparator](#attr-comparator)
    1. [Batch](#batch)
    1. [Block](#block)
    1. [RandNumb](#rand-numb)

# Query Processing Implementation <a name="#implementation"></a>
## Operators <a name="#operators"></a>
### BlockNestedJoin.java <a name="#block-nested-loop-join"></a>
    public class BlockNestedJoin extends Join {
        static int fileNum = 0; // A unique file number for this operation
    
        ObjectInputStream inputStream; // File pointer to the right hand materialized file
        String rTableFileName;  // The file name where the right table is materialize
        Batch outputBuffer;     // Output buffer
    
        Batch leftBatch;        // Buffer for the left input stream
        Batch rightBatch;       // Buffer for the right input stream
    
        int batchSize;          // Number of tuples per out batch, batch = page
        int blockSize;          // Number of batches per block
    
        int leftBufferPtr;      // Cursor for left side buffer
        int rightBufferPtr;     // Cursor for right side buffer
    
        int leftAttrIndex;      // Index of the join attribute in the left table
        int rightAttrIndex;     // Index of the join attribute in the right table
    
        boolean lhsTableEndOfStream;  // Whether the end of stream for the left table is reached
        boolean rhsTableEndOfStream;  // Whether the end of stream for the right table is reached
    
        public BlockNestedJoin(Join join) {
            super(join.getLeft(), join.getRight(), join.getCondition(), join.getOpType());
            super.schema = join.getSchema();
            super.jointype = join.getJoinType();
            super.numBuff = BufferManager.getBuffersPerJoin();
            this.blockSize = numBuff - 2;
        }
    
        public int getBlockSize() {
            return blockSize;
        }
    
        public void setBlockSize(int blockSize) {
            this.blockSize = blockSize;
        }
    
        /**
         * Finds the index of the join attributes
         * Materializes the right hand side into a file
         * Opens the connections
         **/
        public boolean open() {
            // Select number of tuples per batch/page
            int tupleSize = schema.getTupleSize();
            batchSize = Batch.getPageSize() / tupleSize;
    
            Attribute leftAttr = this.getCondition().getLhs();
            leftAttrIndex = left.getSchema().indexOf(leftAttr);
    
            Attribute rightAttr = (Attribute) this.getCondition().getRhs();
            rightAttrIndex = right.getSchema().indexOf(rightAttr);
    
            Batch rightPage;
    
            // Initialize the cursors of input buffers **/
            rightBufferPtr = 0;
            leftBufferPtr = 0;
    
            // Because right stream is to be repetitively scanned if it reached end, we have to start new scan
            rhsTableEndOfStream = true;
            lhsTableEndOfStream = false;
    
            // Right hand side table is to be materialized for the Nested join to perform
            if (!right.open()) {
                return false;
            }
    
            // If the right operator is not a base table then materialize the intermediate result from
            // right into a file
            fileNum++;
            rTableFileName = "BNJtemp-" + fileNum;
            try {
                ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(rTableFileName));
                while( (rightPage = right.next()) != null) {
                    outputStream.writeObject(rightPage);
                }
                outputStream.close();
            } catch(IOException ioe) {
                System.err.println("BlockNested: Writing the temporary file error");
                return false;
            }
    
            if (!right.close()) {
                return false;
            }
    
            return left.open();
        }
    
        /**
         * Selects the tuples satisfying join condition from the input buffers
         * And returns a page of output tuples
         */
        public Batch next() {
            int i, j;
            if (lhsTableEndOfStream) {
                close();
                return null;
            }
            outputBuffer = new Batch(batchSize);
    
            while (!outputBuffer.isFull()) {
    
                if (leftBufferPtr == 0 && rhsTableEndOfStream) {
                    // New left block is to be fetched
                    List<Batch> leftBlock = new ArrayList<>(blockSize);
                    for (int m = 0; m < blockSize; m++) {
                        // Add batch to block
                        Batch batch = left.next();
                        if (batch != null) leftBlock.add(batch);
                    }
    
                    // Adds all tuples in the left block to a new batch
                    leftBatch = addAllTuplesInLeftBlockToNewBatch(blockSize, batchSize, leftBlock);
    
                    if (leftBlock == null || leftBlock.isEmpty()) {
                        lhsTableEndOfStream = true;
                        return outputBuffer;
                    }
    
                    // When a new left block comes, we start scanning the right table
                    scanRightTable(rTableFileName);
                }
    
                while (!rhsTableEndOfStream) {
                    try {
                        if (rightBufferPtr == 0 && leftBufferPtr == 0) {
                            rightBatch = (Batch) inputStream.readObject();
                        }
                        // Join phase
                        // For each block of the outer table, check all tuples of the inner table
                        for (i = leftBufferPtr; i < leftBatch.size(); i++) {
    
                            for (j = rightBufferPtr; j < rightBatch.size(); j++) {
                                Tuple leftTuple = leftBatch.get(i);
                                Tuple rightTuple = rightBatch.get(j);
    
                                if (leftTuple.checkJoin(rightTuple, leftAttrIndex, rightAttrIndex)) {
                                    Tuple outTuple = leftTuple.joinWith(rightTuple);
                                    outputBuffer.add(outTuple);
    
                                    if (outputBuffer.isFull()) {
                                        if (i == leftBatch.size() - 1 && j == rightBatch.size() - 1) {
                                            leftBufferPtr = 0;
                                            rightBufferPtr = 0;
                                        } else if (i != leftBatch.size() - 1 && j == rightBatch.size() - 1) {
                                            leftBufferPtr = i + 1;
                                            rightBufferPtr = 0;
                                        } else if (i == leftBatch.size() - 1 && j != rightBatch.size() - 1) {
                                            leftBufferPtr = i;
                                            rightBufferPtr = j + 1;
                                        } else {
                                            leftBufferPtr = i;
                                            rightBufferPtr = j + 1;
                                        }
    
                                        return outputBuffer;
                                    }
                                }
                            }
    
                            rightBufferPtr = 0;
                        }
    
                        leftBufferPtr = 0;
                    } catch (EOFException eofe) {
                        closeStream();
                        rhsTableEndOfStream = true;
                    } catch (ClassNotFoundException cnfe) {
                        exitProgram("BlockNested: Some error in deserialization");
                    } catch (IOException ioe) {
                        exitProgram("BlockNested: Temporary file reading error");
                    }
                }
            }
    
            return outputBuffer;
        }
    
        private Batch addAllTuplesInLeftBlockToNewBatch(int blockSize, int batchSize, List<Batch> leftBlock) {
            Batch leftBatch = new Batch(blockSize * batchSize);
            for (Batch page : leftBlock) {
                for (int k = 0; k < page.size(); k++) {
                    leftBatch.add(page.get(k));
                }
            }
            return leftBatch;
        }
    
        private void closeStream() {
            try {
                inputStream.close();
            } catch (IOException ioe) {
                System.err.println("BlockNested: Error in temporary file reading ");
            }
        }
    
        // When a new left block comes, we start scanning the right table
        private void scanRightTable(String rTableFileName) {
            try{
                inputStream = new ObjectInputStream(new FileInputStream(rTableFileName));
                rhsTableEndOfStream = false;
            } catch(IOException ioe) {
                exitProgram("BlockNested: Error in reading the file");
            }
        }
    
        private void exitProgram(String message) {
            System.err.println(message);
            System.exit(1);
        }
    
        // Close the operator
        public boolean close() {
            File f = new File(rTableFileName);
            f.delete();
            return true;
        }
    }

### Distinct.java <a name="#distinct"></a>
    public class Distinct extends Operator {
        Operator base;
        ArrayList<Attribute> attrSet;
        ObjectInputStream inputStream;
        List<File> sortedFiles;
        Batch inPage;
        Batch outPage;
        Tuple lastTuple;
        int[] attrIndex;
        int batchSize;
        int numBuff;
        int numRuns;
        // A cursor which positions at the input buffer
        int position;
        boolean endOfStream;
    
        public Distinct(Operator base, ArrayList<Attribute> attrSet, int opType) {
            super(opType);
            this.base = base;
            this.attrSet = attrSet;
        }
    
        public Operator getBase() {
            return base;
        }
    
        public void setBase(Operator base) {
            this.base = base;
        }
    
        public int getNumBuff() {
            return numBuff;
        }
    
        public void setNumBuff(int numBuff) {
            this.numBuff = numBuff;
        }
    
        public boolean open() {
            this.endOfStream = false;
            this.lastTuple = null;
            this.position = 0;
            if (numBuff < 3) {
                System.out.println("Error: Min number of buffer required is 3");
                System.exit(1);
            }
    
            // Sets number of tuples per page
            int tupleSize = base.getSchema().getTupleSize();
    
            batchSize = Batch.getPageSize() / tupleSize;
            Schema baseSchema = base.getSchema();
            attrIndex = fillAttrIndex(attrSet, baseSchema);
    
            if (base.open()) {
                // Generates the sorted runs
                sortedFiles = new ArrayList<>();
                numRuns = 0;
    
                // Reads the tuples into buffer as much as possible
                Batch inBatch = base.next();
                while (inBatch != null && !inBatch.isEmpty()) {
                    Block run = new Block(numBuff, batchSize);
    
                    while (inBatch != null && !inBatch.isEmpty() && !run.isFull()) {
                        run.addBatch(inBatch);
                        inBatch = base.next();
                    }
    
                    numRuns++;
                    Vector<Tuple> tuples = run.getTuples();
                    tuples.sort(new AttrComparator(attrIndex));
                    Block sortedRun = new Block(numBuff, batchSize);
                    sortedRun.setTuples(tuples);
                    File file = writeToFile(sortedRun, numRuns);
                    sortedFiles.add(file);
                }
    
                // Merges the sorted runs
                mergeSortedFiles();
    
                return readFileToInput();
            } else {
                return false;
            }
        }
    
        public Batch next() {
            int i;
    
            if (endOfStream) {
                close();
                return null;
            }
    
            outPage = new Batch(batchSize);
    
            while (!outPage.isFull()) {
                // Reads a new incoming page
                if (position == 0) {
                    inPage = nextBatch(inputStream);
    
                    // Checks if it reaches the end of incoming pages
                    if (inPage == null || inPage.size() == 0) {
                        endOfStream = true;
                        return outPage;
                    }
                }
    
                for (i = position; i < inPage.size(); i++) {
                    Tuple currentTuple = inPage.get(i);
                    if (lastTuple == null) {
                        addCurrentPageToOutPage(currentTuple);
                    } else {
                        int result = compareTuples(currentTuple);
                        if (result != 0) {
                            addCurrentPageToOutPage(currentTuple);
                        }
                    }
                }
    
                position = fixPosition(i);
            }
    
            return outPage;
        }
    
        // Closes the operator
        public boolean close() {
            deleteFiles(sortedFiles);
            try {
                inputStream.close();
            } catch (IOException e) {
                System.err.println("Error: Cannot close input stream");
            }
            return true;
        }
    
        public Object clone() {
            Operator newBase = (Operator) base.clone();
            ArrayList<Attribute> newAttr = (ArrayList<Attribute>) attrSet.clone();
            Distinct newDistinct = new Distinct(newBase, newAttr, optype);
            newDistinct.setSchema(newBase.getSchema());
            return newDistinct;
        }
    
        // Initializes the output stream which writes into resultFile
        private ObjectOutputStream initObjectOutputStream(File resultFile) {
            try {
                return new ObjectOutputStream(new FileOutputStream(resultFile, true));
            } catch (FileNotFoundException e) {
                System.err.println("Error: File not found");
            } catch (IOException e) {
                System.err.println("Initialise Error: Cannot open/write to temp file " + resultFile.getName());
            }
            return null;
        }
    
        // Recursively merge until only left with the last run
        private void mergeSortedFiles() {
            int numInputBuffer = numBuff - 1;
            int instanceNum = 0;
            List<File> resultSortedFiles;
    
            // More than one run is left
            while (sortedFiles.size() > 1) {
                resultSortedFiles = new ArrayList<>();
                int numMergeRuns = 0;
    
                for (int i = 0; i * numInputBuffer < sortedFiles.size(); i++) {
                    // Number of files being sorted equals to the size of input buffers
                    int start = i * numInputBuffer;
                    int end = start + numInputBuffer;
    
                    // The last run may not use up all the input buffers
                    if (end >= sortedFiles.size()) {
                        end = sortedFiles.size();
                    }
    
                    List<File> filesToBeSorted = sortedFiles.subList(start, end);
                    File singleSortedFile = mergeSortedRuns(filesToBeSorted, instanceNum, numMergeRuns);
                    numMergeRuns++;
                    resultSortedFiles.add(singleSortedFile);
                }
    
                deleteFiles(sortedFiles);
                sortedFiles = resultSortedFiles;
                instanceNum++;
            }
        }
    
        private File mergeSortedRuns(List<File> runs, int instanceNumber, int numMergeRuns) {
            // A cursor pointing at the index of list of runs
            int runIndex;
    
            int numInputBuffer = numBuff - 1;
            boolean hasAdditionalBuffer = (numInputBuffer > runs.size());
    
            if (numInputBuffer < runs.size()) {
                System.err.println("Error: Number of runs exceeds capacity of input buffer. Sorting terminates.");
                return null;
            }
    
            List<ObjectInputStream> inputStreams = readRunsToInputList(runs);
    
            // Start the merging
            File resultFile = new File("Run-" + instanceNumber + "-" + numMergeRuns);
            ObjectOutputStream outputStream = initObjectOutputStream(resultFile);
    
            // Reads in the input streams to get the page and adds the page to the input buffer
            List<Batch> inBuffers = readPageToInput(runs, inputStreams);
    
            // Writes the results to output stream
            Batch outBuffer = new Batch(batchSize);
            Batch tempBatch;
    
            if (hasAdditionalBuffer) {
                Queue<Tuple> inputTuples = new PriorityQueue<>(runs.size(), new AttrComparator(attrIndex));
                Map<Tuple, Integer> tupleRunIndexMap = new HashMap<>(runs.size());
    
                for (int j = 0; j < runs.size(); j++) {
                    tempBatch = inBuffers.get(j);
                    Tuple tuple = tempBatch.get(0);
                    tupleRunIndexMap.put(tuple, j);
                    inputTuples.add(tuple);
                    tempBatch.remove(0);
    
                    if (tempBatch.isEmpty()) {
                        tempBatch = nextBatch(inputStreams.get(j));
                        inBuffers.set(j, tempBatch);
                    }
                }
    
                while (!inputTuples.isEmpty()) {
                    Tuple minTuple = inputTuples.remove();
                    outBuffer.add(minTuple);
    
                    // Writes the entries in the output buffer to output stream
                    outputTuples(outBuffer, outputStream);
    
                    // Extracts another tuple from the same run until there are no more tuples in this run
                    // and add the tuple into the queue
                    runIndex = tupleRunIndexMap.get(minTuple);
                    tempBatch = inBuffers.get(runIndex);
                    if (tempBatch != null) {
                        Tuple tuple = tempBatch.get(0);
                        tupleRunIndexMap.put(tuple, runIndex);
                        inputTuples.add(tuple);
                        tempBatch.remove(0);
    
                        if (tempBatch.isEmpty()) {
                            tempBatch = nextBatch(inputStreams.get(runIndex));
                            inBuffers.set(runIndex, tempBatch);
                        }
                    }
                }
    
                // Adds the remaining tuples in output buffer to output stream
                outputRemainingTuples(outBuffer, outputStream);
    
                tupleRunIndexMap.clear();
            } else {
                while (!completesExtraction(inBuffers)) {
                    runIndex = getIndexOfMinTuple(inBuffers);
                    tempBatch = inBuffers.get(runIndex);
    
                    // Adds the minTuple to output buffer
                    Tuple minTuple = tempBatch.get(0);
                    tempBatch.remove(0);
                    outBuffer.add(minTuple);
    
                    // Writes the result in output buffer into out stream
                    outputTuples(outBuffer, outputStream);
    
                    if (tempBatch.isEmpty()) {
                        tempBatch = nextBatch(inputStreams.get(runIndex));
                        inBuffers.set(runIndex, tempBatch);
                    }
                }
    
                // Adds the remaining tuples in output buffer to output stream
                outputRemainingTuples(outBuffer, outputStream);
            }
    
            try {
                outputStream.close();
            } catch (IOException e) {
                System.err.println("Error: Unable to close output stream");
            }
    
            return resultFile;
        }
    
        // Returns the index of the minimum tuple in the input buffer
        private int getIndexOfMinTuple(List<Batch> inBuffers) {
            Tuple minTuple = null;
            int minIndex = 0;
    
            // Gets the first non-null tuple in the input buffer
            for (int i = 0; i < inBuffers.size(); i++) {
                if (inBuffers.get(i) != null) {
                    minTuple = inBuffers.get(i).get(0);
                    minIndex = i;
                }
            }
    
            // Compares the entire input buffer to find the actual min
            for (int j = 0; j < inBuffers.size(); j++) {
                if (inBuffers.get(j) != null) {
                    Tuple current = inBuffers.get(j).get(0);
                    int result = 0;
    
                    for (int index : attrIndex) {
                        result = Tuple.compareTuples(current, minTuple, index, index);
                        if (result != 0) {
                            break;
                        }
                    }
    
                    if (result < 0) {
                        minTuple = current;
                        minIndex = j;
                    }
                }
            }
    
            return minIndex;
        }
    
        // Checks if all pages in the input buffer have been extracted, i.e. whether all pages are null
        private boolean completesExtraction(List<Batch> inBuffers) {
            for (Batch inBuffer : inBuffers) {
                if (inBuffer != null) {
                    return false;
                }
            }
            return true;
        }
    
        private int[] fillAttrIndex(ArrayList<Attribute> attrSet, Schema baseSchema) {
            int[] attrIndex = new int[attrSet.size()];
            for (int i = 0; i < attrSet.size(); i++) {
                Attribute a = attrSet.get(i);
                int id = baseSchema.indexOf(a);
                attrIndex[i] = id;
            }
            return attrIndex;
        }
    
        private void addCurrentPageToOutPage(Tuple current) {
            outPage.add(current);
            lastTuple = current;
        }
    
        private int fixPosition(int i) {
            if (i == inPage.size()) {
                return position = 0;
            } else {
                return position = i;
            }
        }
    
        private int compareTuples(Tuple currentTuple) {
            int result = 0;
            for (int index : attrIndex) {
                result = Tuple.compareTuples(lastTuple, currentTuple, index, index);
                if (result != 0) {
                    break;
                }
            }
            return result;
        }
    
        // Writes the result in output buffer into out stream
        private void outputTuples(Batch outBuffer, ObjectOutputStream outputStream) {
            if (outBuffer.isFull()) {
                try {
                    outputStream.writeObject(outBuffer);
                    outputStream.reset();
                } catch (IOException e) {
                    System.err.println("Sort Merge Error: Cannot write to output steam");
                }
                outBuffer.clear();
            }
        }
    
        // Adds the remaining tuples in output buffer to output stream
        private void outputRemainingTuples(Batch outBuffer, ObjectOutputStream outputStream) {
            if (!outBuffer.isEmpty()) {
                try {
                    outputStream.writeObject(outBuffer);
                    outputStream.reset();
                } catch (IOException e) {
                    System.err.println("Sort Merge Error: Cannot write to output steam");
                }
                outBuffer.clear();
            }
        }
    
        // Returns a batch of tuples
        private Batch nextBatch(ObjectInputStream inputStream) {
            if (sortedFiles.size() != 1) {
                System.err.println("Error: Incorrectly sorted");
            }
            // Returns next page if there are still pages available
            try {
                Batch batch = (Batch) inputStream.readObject();
                if (batch == null) {
                    System.err.println("Note: Batch is null");
                }
                return batch;
            } catch (IOException e) {
                return null;
            } catch (ClassNotFoundException e) {
                System.err.println("Error: Class not found");
            }
            return null;
        }
    
        private boolean readFileToInput() {
            try {
                inputStream = new ObjectInputStream(new FileInputStream(sortedFiles.get(0)));
            } catch (FileNotFoundException e) {
                System.err.println("Error: File " + sortedFiles.get(0) + " not found");
                return false;
            } catch (IOException e) {
                System.err.println("Error: Cannot read file " + sortedFiles.get(0));
                return false;
            }
            return true;
        }
    
        // Reads the runs into a list of input streams
        private List<ObjectInputStream> readRunsToInputList(List<File> runs) {
            List<ObjectInputStream> inputStreams = new ArrayList<>();
            try {
                for (int i = 0; i < runs.size(); i++) {
                    ObjectInputStream inputStream = new ObjectInputStream((new FileInputStream(runs.get(i))));
                    inputStreams.add(inputStream);
                }
            } catch (FileNotFoundException e) {
                System.err.println("Error: File not found");
            } catch (IOException e) {
                System.err.println("MSR Error: Cannot open/write to temp file");
            }
            return inputStreams;
        }
    
        // Adds the pages to the input buffer
        private List<Batch> readPageToInput(List<File> runs, List<ObjectInputStream>  inputStreams) {
            List<Batch> inBuffers = new ArrayList<>(runs.size());
            for (int i = 0; i < runs.size(); i++) {
                Batch batch = nextBatch(inputStreams.get(i));
                if (batch == null) {
                    System.err.println("Merging Error: Run-" + i + " is empty");
                }
                inBuffers.add(i, batch);
            }
            return inBuffers;
        }
    
        // Writes the pages of sorted run into the file
        private File writeToFile(Block sortedRun, int numRuns) {
            try {
                File tempFile = new File("SMTemp-" + numRuns);
                ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(tempFile));
                for (Batch batch : sortedRun.getBatches()) {
                    outputStream.writeObject(batch);
                }
                outputStream.close();
                return tempFile;
            } catch (FileNotFoundException e) {
                System.err.println("Error: File not found");
            } catch (IOException e) {
                System.err.println("WriteToFile Error: Cannot open/write to temp file");
            }
            return null;
        }
    
        private void deleteFiles(List<File> sortedFiles) {
            for (File file : sortedFiles) {
                file.delete();
            }
        }
    }


### GroupBy.java <a name="#group-by"></a>
    public class GroupBy extends Operator {
        Operator base;
        Batch outBatch;
        ArrayList<Attribute> attrSet;
        ArrayList<List<Integer>> passCounts;
    
        // File names of all partitions generated during partition phase
        ArrayList<String> fileNames = new ArrayList<>();
    
        int numBuffer;
    
        // number of tuples per outBatch
        int batchSize;
    
        // To get a unique file number for this operation
        static int fileNum = -1;
    
        // Group By instance unique file num (to be used to generate output file name)
        int currentFileNum;
    
        // To resume page reading after each next
        int pageCursor;
    
        // Index of the attributes in the base operator that are to be group by
        int[] attrIndex;
    
        public GroupBy(Operator base, ArrayList<Attribute> as, int type) {
            super(type);
            this.base = base;
            this.attrSet = as;
        }
    
        public void setBase(Operator base) {
            this.base = base;
        }
    
        public Operator getBase() {
            return base;
        }
    
        public ArrayList<Attribute> getGroupAttr() {
            return attrSet;
        }
    
        // Opens the connection to the base operator
        // Also figures out what are the columns to be projected from the base operator
        public boolean open() {
            // Sets the number of tuples per batch
            int tupleSize = schema.getTupleSize();
            batchSize = Batch.getPageSize() / tupleSize;
            // The minimum number of buffers required
            numBuffer = 3;
            pageCursor = 0;
            currentFileNum = ++fileNum;
            passCounts = new ArrayList<>();
    
            // The following loop finds out the index of the columns that are required from the base operator
            Schema baseSchema = base.getSchema();
            attrIndex = new int[attrSet.size()];
    
            for (int i = 0; i < attrSet.size(); i++) {
                Attribute attr = attrSet.get(i);
                int index = baseSchema.indexOf(attr);
                attrIndex[i] = index;
            }
    
            if (!base.open()) {
                return false;
            }
    
            /*
             * Sorting phase - In-memory sorting
             * Post-condition: Generate sorted initial run
             */
            Batch[] buffers = new Batch[numBuffer];
    
            buffers[0] = base.next();
            List<Integer> runCounts = new ArrayList<>();
            // Pass 0, # of runs produced
            passCounts.add(runCounts);
            int initialRun = -1;
    
            while (buffers[0] != null) {
                initialRun++;
                // Fill all buffers
                fillBuffers(buffers);
    
                // Extract all lists from tuples in the batches
                // TreeSet sort and eliminate duplicates
                TreeSet<ArrayList<Object>> orderedLists = new TreeSet<>(getComparator());
                for (Batch buffer : buffers) {
                    if (buffer == null) {
                        break;
                    }
    
                    Iterator<Tuple> iterator = buffer.getIterator();
                    while (iterator.hasNext()) {
                        orderedLists.add(getGroupByValues(iterator.next()));
                    }
                }
    
                // Write initial runs
                Iterator<ArrayList<Object>> iterator = orderedLists.iterator();
                buffers[0] = new Batch(batchSize);
                int pageNum = -1;
                while (iterator.hasNext()) {
                    Tuple t = new Tuple(iterator.next());
                    buffers[0].add(t);
                    Debug.PPrint(t);
    
                    if (buffers[0].isFull()) {
                        pageNum++;
                        pageWrite(buffers[0], generateFileName(0, initialRun, pageNum));
                        buffers[0].clear();
                    }
                }
    
                if (!buffers[0].isEmpty()) {
                    pageNum++;
                    pageWrite(buffers[0], generateFileName(0, initialRun, pageNum));
                }
    
                runCounts.add(initialRun, pageNum + 1);
                buffers[0] = base.next();
            }
    
            // Sorting phase (into single run)
            for (int pass = 0; pass < passCounts.size(); pass++) {
                // For each pass
                List<Integer> preRunCounts = passCounts.get(pass);
                if (preRunCounts.size() < 2) {
                    // Single run achieved
                    return true;
                }
    
                // Output buffer
                buffers[buffers.length - 1] = new Batch(batchSize);
                List<Integer> curRunCount = new ArrayList<>();
                passCounts.add(curRunCount);
                int runCursors = -1;
                int newRunCount = 0;
    
                while (runCursors + 1 < preRunCounts.size()) {
                    // New run in progress
                    // Each iterator used up a single batch page
                    List<Iterator<Tuple>> iterators = new ArrayList<>(numBuffer - 1);
    
                    for (int i = runCursors + 1; i < preRunCounts.size(); i++) {
                        if (iterators.size() == numBuffer - 1) {
                            break;
                        }
                        runCursors = i;
                        // Load all runs into b -1 buffers
                        Iterator<Tuple> iterator = new TupleIterator(generateFileNamePrefix(pass, runCursors),
                                preRunCounts.get(runCursors));
                        iterators.add(iterator);
                    }
    
                    int pagesGenerated = merge(iterators, buffers[buffers.length - 1], pass + 1, newRunCount);
                    curRunCount.add(newRunCount, pagesGenerated);
                    newRunCount++;
                    iterators = new ArrayList<>(numBuffer - 1);
                    // End of one new run
                }
                // End of a pass
            }
    
            return true;
        }
    
        private void fillBuffers(Batch[] buffers) {
            for (int i = 1; i < buffers.length; i++) {
                buffers[i] = base.next();
                if (buffers[i] == null) {
                    break;
                }
            }
        }
    
        /**
         * Read next tuple from operator
         */
        public Batch next() {
            outBatch = null;
    
            int pass = passCounts.size() - 1;
            int run = passCounts.get(pass).size() - 1;
            if (run > 0) {
                System.out.println("Runs not merged!");
            }
            int pages = passCounts.get(pass).get(run);
    
            if (pageCursor < pages) {
                // Each file read match with batch size
                pageRead(generateFileName(pass, run, pageCursor));
                pageCursor++;
            }
    
            return outBatch;
        }
    
    
        /**
         * Close the operator
         */
        public boolean close() {
            for (int i = 0; i < fileNames.size(); i++) {
                File f = new File(fileNames.get(i));
                f.delete();
            }
    
            return base.close();
        }
    
    
        public Object clone() {
            Operator newBase = (Operator) base.clone();
            ArrayList<Attribute> newAttr = new ArrayList<>();
            for (Attribute attribute : attrSet) {
                newAttr.add((Attribute) attribute.clone());
            }
            GroupBy newProject = new GroupBy(newBase, newAttr, optype);
            Schema newSchema = newBase.getSchema().subSchema(newAttr);
            newProject.setSchema(newSchema);
            return newProject;
        }
    
        private ArrayList<Object> getGroupByValues(Tuple tuple) {
            ArrayList<Object> values = new ArrayList<>();
            for (int i : attrIndex) {
                values.add(tuple.dataAt(i));
            }
            return values;
        }
    
        /**
         * Batch write out.
         *
         * @param batch        page to be written out
         * @param fileName name of the file
         */
        private boolean pageWrite(Batch batch, String fileName) {
            try {
                ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(fileName));
                outputStream.writeObject(batch);
                outputStream.close();
                fileNames.add(fileName);
            } catch (IOException ioe) {
                System.err.println("Group By: Writing the temporary file error === " + fileName);
                return false;
            }
            return true;
        }
    
        private boolean pageRead(String fileName) {
            ObjectInputStream inputStream;
            try {
                inputStream = new ObjectInputStream(new FileInputStream(fileName));
            } catch (IOException ioe) {
                System.err.println("Group By: Error in reading the file === " + fileName);
                return false;
            }
    
            try {
                outBatch = (Batch) inputStream.readObject();
                inputStream.close();
            } catch (EOFException eofe) {
                try {
                    inputStream.close();
                } catch (IOException ioe) {
                    System.err.println("Group By: Error in temporary file reading === " + fileName);
                }
            } catch (ClassNotFoundException cnfe) {
                exitProgram("Group By: Some error in deserialization  === " + fileName);
            } catch (IOException ioe) {
                exitProgram("Group By: Temporary file reading error  === " + fileName);
            }
    
            return true;
        }
    
        private void exitProgram(String message) {
            System.err.println(message);
            System.exit(1);
        }
    
        /**
         * File name generator to standardize file name used in I/O
         *
         * @return file name generated
         */
        private String generateFileName(int passNum, int runNum, int pageNum) {
            return String.format("GBtemp%d-pass%d-run%d.%d", currentFileNum, passNum, runNum, pageNum);
        }
    
        private String generateFileNamePrefix(int passNum, int runNum) {
            return String.format("GBtemp%d-pass%d-run%d.", currentFileNum, passNum, runNum);
        }
    
        private Comparator<ArrayList<Object>> getComparator() {
            return (x, y) -> {
                for (int i = 0; i < x.size(); i++) {
                    Object leftData = x.get(i);
                    Object rightData = y.get(i);
                    int comparison;
    
                    if (leftData instanceof Integer) {
                        comparison = ((Integer) leftData).compareTo((Integer) rightData);
                    } else if (leftData instanceof String) {
                        comparison = ((String) leftData).compareTo((String) rightData);
                    } else if (leftData instanceof Float) {
                        comparison = ((Float) leftData).compareTo((Float) rightData);
                    } else {
                        exitProgram("Tuple: Unknown comparision of the tuples");
                        return 0;
                    }
    
                    // Attribute is not equal (differences found)
                    // If Attribute is equal (continue to compare the next attribute)
                    if (comparison != 0) {
                        return comparison;
                    }
                }
                return 0;
            };
        }
    
        private int merge(List<Iterator<Tuple>> iterators, Batch output, int nextPass, int run) {
            int pageNum = -1;
            TreeMap<ArrayList<Object>, Integer> map = new TreeMap<>(getComparator());
    
            // Initial loading
            for (int i = 0; i < iterators.size(); i++) {
                Iterator<Tuple> iterator = iterators.get(i);
                while (iterator.hasNext()) {
                    ArrayList<Object> value = iterator.next().data();
                    if (!map.containsKey(value)) {
                        // No duplicate, go to next iterator
                        map.put(value, i);
                        break;
                    }
                }
            }
    
            // Polling of tuples and replacing
            while (!map.isEmpty()) {
                Map.Entry<ArrayList<Object>, Integer> outputList = map.pollFirstEntry();
                // Put in batch, write if full
                output.add(new Tuple(outputList.getKey()));
                if (output.isFull()) {
                    pageNum++;
                    writeToOutput(pageNum, output, nextPass, run);
                }
                // Replace with next tuple from that particular run
                int index = outputList.getValue();
                Iterator<Tuple> iterator = iterators.get(index);
                while (iterator.hasNext()) {
                    ArrayList<Object> value = iterator.next().data();
                    if (!map.containsKey(value)) {
                        // No duplicate, go to next iterator
                        map.put(value, index);
                        break;
                    }
                }
            }
    
            if (!output.isEmpty()) {
                pageNum++;
                writeToOutput(pageNum, output, nextPass, run);
            }
    
            return pageNum + 1;
        }
    
        private void writeToOutput(int pageNum, Batch output, int nextPass, int run) {
            pageWrite(output, generateFileName(nextPass, run, pageNum));
            System.out.println(generateFileName(nextPass, run, pageNum));
            Debug.PPrint(output);
            output.clear();
        }
    }

### HashJoin.java <a name="#hash-join"></a>
    public class HashJoin extends Join {
        static int fileNum = -1;   // To get unique file num for this operation
    
        final int PRIME1;
        final int PRIME2;
        final int PRIME3;
        final int PRIME4;
    
        Batch inputBuffer;
        Batch outputBuffer;     // Used in probing phase
        Batch[] hashTable;      // Build in probing phase
        Join recursiveJoin;
    
        List<String> fileNames = new ArrayList<>(); // File names of all partitions generated during partition phase
        List<Integer> failedPartition = new ArrayList<>();
    
        int[] partitionLeftPageCounts;      // File count per partition
        int[] partitionsRightPageCounts;    // File count per partition
    
        int leftIndex;     // Index of the join attribute in left table
        int rightIndex;    // Index of the join attribute in right table
    
        int lTupleCursor; // Last checked tuple in a particular bucket of the hash table
        int rTupleCursor; // Last probed tuple in the right page
    
        int currentFileNum; // Hash Join instance unique file num (to be used to generate output file name)
        int recursiveCount;
        int currPartitionCursor; // Current partition cursor
        int rPageCursor; // Current right page cursor
    
        boolean done;
    
        public HashJoin(Join join) {
            super(join.getLeft(), join.getRight(), join.getCondition(), join.getOpType());
            schema = join.getSchema();
            jointype = join.getJoinType();
            numBuff = join.getNumBuff();
            recursiveCount = 0;
            PRIME1 = RandNumb.randPrime();
            int tempPrime = RandNumb.randPrime();
            while (PRIME1 == tempPrime) {
                tempPrime = RandNumb.randPrime();
            }
            PRIME2 = tempPrime;
            while (PRIME1 == tempPrime || PRIME2 == tempPrime) {
                tempPrime = RandNumb.randPrime();
            }
            PRIME3 = tempPrime;
            while (PRIME1 == tempPrime || PRIME2 == tempPrime || PRIME3 == tempPrime) {
                tempPrime = RandNumb.randPrime();
            }
            PRIME4 = tempPrime;
        }
    
        public HashJoin(Join join, int recursiveCount) {
            super(join.getLeft(), join.getRight(), join.getCondition(), join.getOpType());
            schema = join.getSchema();
            jointype = join.getJoinType();
            numBuff = join.getNumBuff();
            this.recursiveCount = recursiveCount;
            PRIME1 = RandNumb.randPrime();
            int tempPrime = RandNumb.randPrime();
            while (PRIME1 == tempPrime) {
                tempPrime = RandNumb.randPrime();
            }
            PRIME2 = tempPrime;
            while (PRIME1 == tempPrime || PRIME2 == tempPrime) {
                tempPrime = RandNumb.randPrime();
            }
            PRIME3 = tempPrime;
            while (PRIME1 == tempPrime || PRIME2 == tempPrime || PRIME3 == tempPrime) {
                tempPrime = RandNumb.randPrime();
            }
            PRIME4 = tempPrime;
        }
    
        public boolean open() {
            currentFileNum = ++fileNum;
            Attribute leftAttr = this.getCondition().getLhs();
            leftIndex = left.getSchema().indexOf(leftAttr);
    
            Attribute rightAttr = (Attribute) this.getCondition().getRhs();
            rightIndex = right.getSchema().indexOf(rightAttr);
    
            // Partition buffers
            Batch[] partitions = new Batch[numBuff - 1];
            partitionLeftPageCounts = new int[numBuff - 1];
            partitionsRightPageCounts = new int[numBuff - 1];
    
            // Partition for left
            if (!partition(partitions, partitionLeftPageCounts, left, leftIndex)) {
                return false;
            }
    
            // Partition for right
            if (!partition(partitions, partitionsRightPageCounts, right, rightIndex)) {
                return false;
            }
    
            // Destroy partition buffers
            partitions = null;
    
            // Setup Buffer for probing phase's hash table
            inputBuffer = new Batch(Batch.getPageSize() / left.schema.getTupleSize());
            hashTable = new Batch[numBuff - 2];
            for (int i = 0; i < numBuff - 2; i++) {
                hashTable[i] = new Batch(Batch.getPageSize() / left.schema.getTupleSize());
            }
    
            // Build first hash table (from a non-empty partition)
            currPartitionCursor = -1;
            for (int i = 0; i < partitionLeftPageCounts.length; i++) {
                if (buildHashTable(i)) {
                    currPartitionCursor = i;
                    break;
                }
            }
    
            rPageCursor = 0;
            rTupleCursor = -1;
            lTupleCursor = -1;
            done = false;
    
            // Prepare buffer for reading of right and output of joined tuples
            inputBuffer = new Batch(Batch.getPageSize() / right.schema.getTupleSize());
            outputBuffer = new Batch(Batch.getPageSize() / schema.getTupleSize());
    
            return true;
        }
    
        // From input buffers selects the tuples satisfying join condition and returns a page of output tuples
        public Batch next() {
            // Prepare (clean) output buffer for new set of joined tuples
            outputBuffer.clear();
    
            for (int parti = currPartitionCursor; parti < partitionLeftPageCounts.length && parti >= 0; parti++) {
                // BUILDING PHASE
                if (parti != currPartitionCursor) {
                    // Probing partition[parti], but hash table (is at partitionCurs) not updated
                    if (buildHashTable(parti)) {
                        currPartitionCursor = parti;
                    } else {
                        // Error reading partition or no tuple in the left of this partition
                        // Move to next partition to build hash table
                        currPartitionCursor = parti;
                        continue;
                    }
                }
    
                // PROBING PHASE
                // Read all pages of the partition from right
                for (int p = rPageCursor; p < partitionsRightPageCounts[currPartitionCursor]; p++) {
                    // For each page of a particular right partition
                    rPageCursor = p;
                    // Read the page
                    String fileName = generateFileName(currPartitionCursor, p, false);
                    if (!pageRead(fileName)) {
                        exitProgram("Error reading partition");
                    }
    
                    // For each tuple
                    for (int t = rTupleCursor + 1; t < inputBuffer.size(); t++) {
                        rTupleCursor = t;
                        Tuple tuple = inputBuffer.get(t);
                        // Find match(es) in hash table
                        Tuple foundTupleLeft;
                        while ((foundTupleLeft = findMatchInHashtable(hashTable, leftIndex, tuple, rightIndex)) != null) {
                            outputBuffer.add(foundTupleLeft.joinWith(tuple));
                            // Return output buffer, if full
                            if (outputBuffer.isFull()) {
                                // rPageCurs = p;
                                rTupleCursor = t - 1; // Come back to this tuple as there might be other matches in the
                                // same bucket
                                return outputBuffer;
                            }
                        }
                    }
                    // End of page, reset tuple curs
                    rTupleCursor = -1;
                }
                // End of partition, reset page and tuple curs
                rPageCursor = 0;
                rTupleCursor = -1;
            }
            currPartitionCursor = Integer.MAX_VALUE;
            if (!outputBuffer.isEmpty()) {
                return outputBuffer;
            }
    
            // End of probing
            if (recursiveHashJoin()) {
                return outputBuffer;
            }
    
            done = true;
            close();
            return null;
        }
    
    
        // Close the operator
        public boolean close() {
            // Delete all partition files
            for (String fileName : fileNames) {
                File f = new File(fileName);
                f.delete();
            }
            inputBuffer = null;
            hashTable = null;
            return true;
        }
    
        /**
         * Hash function used for partitioning.
         *
         * @param tuple      the tuple
         * @param index      the index of the attribute to be hashed
         * @param bucketSize the size of the partition
         * @return partition to be hashed
         */
        private int hashFunction1(Tuple tuple, int index, int bucketSize) {
            Object value = tuple.dataAt(index);
            int hashValue = Objects.hash(value);
            return ((hashValue * PRIME1) % PRIME2) % bucketSize;
        }
    
        /**
         * Hash function used for probing.
         *
         * @param tuple      the tuple
         * @param index      the index of the attribute to be hashed
         * @param bucketSize the size of the buckets in the hash table
         * @return bucket to be hashed
         */
        private int hashFunction2(Tuple tuple, int index, int bucketSize) {
            Object value = tuple.dataAt(index);
            int hashValue = Objects.hash(value);
            return ((hashValue * PRIME3) % PRIME4) % bucketSize;
        }
    
        /**
         * Batch write out.
         *
         * @param batch    page to be written out
         * @param fileName name of the file
         */
        private boolean pageWrite(Batch batch, String fileName) {
            try {
                ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(fileName));
                outputStream.writeObject(batch);
                fileNames.add(fileName);
                outputStream.close();
            } catch (IOException ioe) {
                System.err.println("Hash Join: Writing the temporary file error === " + fileName);
                return false;
            }
            return true;
        }
    
        /**
         * Read batch into input buffer.
         *
         * @param fileName name of the file to be read
         * @return True if page has been read into input buffer
         */
        private boolean pageRead(String fileName) {
            ObjectInputStream inputStream;
            try {
                inputStream = new ObjectInputStream(new FileInputStream(fileName));
            } catch (IOException ioe) {
                System.err.println("Hash Join: Error in reading the file === " + fileName);
                return false;
            }
    
            try {
                inputBuffer = (Batch) inputStream.readObject();
                inputStream.close();
            } catch (EOFException eofe) {
                try {
                    inputStream.close();
                } catch (IOException ioe) {
                    System.err.println("Hash Join: Error in temporary file reading === " + fileName);
                }
            } catch (ClassNotFoundException cnfe) {
                exitProgram("Hash Join: Some error in deserialization  === " + fileName);
            } catch (IOException ioe) {
                exitProgram("Hash Join: Temporary file reading error  === " + fileName);
            }
    
            return true;
        }
    
        /**
         * File name generator to standardize file name used in I/O
         *
         * @param partitionNum the partition of the tuple hashed
         * @param pageNum      running page number from 0 to end of batch of the partition
         * @param isLeft       the original operator of the tuples in stored file
         * @return file name generated
         */
        private String generateFileName(int partitionNum, int pageNum, boolean isLeft) {
            if (isLeft) {
                return String.format("HJtemp%d-LeftPartition%d-%d", currentFileNum, partitionNum, pageNum);
            } else {
                return String.format("HJtemp%d-RightPartition%d-%d", currentFileNum, partitionNum, pageNum);
            }
        }
    
        private String generateFileNamePrefix(int partitionNum, boolean isLeft) {
            if (isLeft) {
                return String.format("HJtemp%d-LeftPartition%d-", currentFileNum, partitionNum);
            } else {
                return String.format("HJtemp%d-RightPartition%d-", currentFileNum, partitionNum);
            }
        }
    
        /**
         * Generate partitions of a operator (left or right) during partitioning phase.
         *
         * @param partitions      output buffers (each represent a partition to be written when full)
         * @param partitionsCount Count of each partition (to be updated when partition count increases)
         * @param op              the operator to be partitioned
         * @param index           the index of the attribute to be hashed
         * @return true if partition is successful, or else false
         */
        private boolean partition(Batch[] partitions, int[] partitionsCount, Operator op, int index) {
            boolean isLeft = (op == left);
    
            setupPartitions(partitions, partitionsCount, op);
    
            if (!op.open()) {
                return false;
            }
    
            while ((inputBuffer = op.next()) != null) {
                for (int i = 0; i < inputBuffer.size(); i++) {
                    Tuple tuple = inputBuffer.get(i);
                    int bucket = hashFunction1(tuple, index, partitions.length);
                    Batch partition = partitions[bucket];
                    if (partition.isFull()) {
                        String fileName = generateFileName(bucket, partitionsCount[bucket], isLeft);
                        pageWrite(partition, fileName);
                        partitionsCount[bucket]++;
                        partition.clear();
                    }
                    partition.add(tuple);
                }
            }
    
            // Write out all non-full partitions
            writeAllPartition(partitions, partitionsCount, isLeft);
    
            return op.close();
        }
    
        private void writeAllPartition(Batch[] partitions, int[] partitionsCount, boolean isLeft) {
            for (int i = 0; i < partitions.length; i++) {
                Batch partition = partitions[i];
                if (partition.isEmpty()) {
                    continue;
                }
                String fileName = generateFileName(i, partitionsCount[i], isLeft);
                pageWrite(partition, fileName);
                partitionsCount[i]++;
            }
        }
    
        /**
         * Build hash table for a specific partition in preparation for probing.
         *
         * @param partitionNum the partition to build the hash table for
         * @return True if hash table is built successfully, else false
         */
        private boolean buildHashTable(int partitionNum) {
            // Array out of bound, ensures partition exist
            if (partitionNum >= partitionLeftPageCounts.length) {
                return false;
            }
    
            // No records in partition to build hash table
            int pageCount = partitionLeftPageCounts[partitionNum];
            if (pageCount < 1) {
                return false;
            }
    
            // Clean old hash table tuples
            cleanHashTable(hashTable);
    
            // Read all pages of the partition from left
            return readPartitionFromLeft(pageCount, partitionNum);
        }
    
        // Read all pages of the partition from left
        private boolean readPartitionFromLeft(int pageCount, int partitionNum) {
            for (int p = 0; p < pageCount; p++) {
                // Read a page of a partition
                String fileName = generateFileName(partitionNum, p, true);
                if (!pageRead(fileName)) {
                    exitProgram("Error reading partition");
                }
    
                // Populate hash table
                for (int t = 0; t < inputBuffer.size(); t++) {
                    Tuple tuple = inputBuffer.get(t);
                    int hash = hashFunction2(tuple, leftIndex, hashTable.length);
                    if (hashTable[hash].isFull()) {
                        failedPartition.add(partitionNum);
                        System.out.println("Hash table (bucket: " + hash + ") is too small. To be recursively hashed");
                        return false;
                    }
                    hashTable[hash].add(tuple, hashTable[hash].size());
                }
            }
    
            return true;
        }
    
        private void exitProgram(String message) {
            System.err.println(message);
            System.exit(1);
        }
    
        private void setupPartitions(Batch[] partitions, int[] partitionsCount, Operator op) {
            int tupleCount = Batch.getPageSize() / op.schema.getTupleSize();
    
            for (int i = 0; i < numBuff - 1; i++) {
                partitions[i] = new Batch(tupleCount);
                partitionsCount[i] = 0;
            }
        }
    
        // Clean old hash table tuples
        private void cleanHashTable(Batch[] hashTable) {
            for (int i = 0; i < numBuff - 2; i++) {
                hashTable[i].clear();
            }
        }
    
        /**
         * Find match in a bucket of the hash table.
         *
         * @param hashTable     the hash table to be search in
         * @param hashAttrIndex attribute index to be match against tuple in the hash table
         * @param tuple         search tuple
         * @param tupleIndex    attribute index to be match against for the tuple
         * @return matching joined tuple
         */
        private Tuple findMatchInHashtable(Batch[] hashTable, int hashAttrIndex, Tuple tuple, int tupleIndex) {
            int hash = hashFunction2(tuple, tupleIndex, hashTable.length);
    
            // Search for match in a specific bucket of the hash table
            for (int i = lTupleCursor + 1; i < hashTable[hash].size(); i++) {
                Tuple tupleInHash = hashTable[hash].get(i);
                lTupleCursor = i;
                if (tupleInHash.checkJoin(tuple, hashAttrIndex, tupleIndex)) {
                    // Return back to curs to continue search
                    return tupleInHash;
                }
            }
            // End of search, reset curs
            lTupleCursor = -1;
            return null;
        }
    
        private boolean recursiveHashJoin() {
            while (failedPartition.size() > 0) {
                int failedPartitionNum = failedPartition.get(0);
    
                if (recursiveJoin == null) {
                    PartitionScan scanLeft = scanLeftPartition(failedPartitionNum);
                    PartitionScan scanRight = scanRightPartition(failedPartitionNum);
                    if (recursiveCount > 3) {
                        System.out.println("3 recursive hash joins has encountered lack of buffer in hash table. " +
                                "Partition will be merged using block nested loop instead.");
                        setupBlockNestedJoin(scanLeft, scanRight);
                    } else {
                        setupRecursiveHashJoin(scanLeft, scanRight);
                    }
                }
    
                outputBuffer = recursiveJoin.next();
    
                if (outputBuffer == null) {
                    closeOutputBuffer();
                } else {
                    // Join tuples found
                    return true;
                }
            }
            return false;
        }
    
        private PartitionScan scanLeftPartition(int failedPartitionNum) {
            PartitionScan scanLeft = new PartitionScan(generateFileNamePrefix(failedPartitionNum, true),
                    partitionLeftPageCounts[failedPartitionNum], OpType.SCAN);
            scanLeft.setSchema(left.schema);
            return scanLeft;
        }
    
        private PartitionScan scanRightPartition(int failedPartitionNum) {
            PartitionScan scanRight = new PartitionScan(generateFileNamePrefix(failedPartitionNum, false),
                    partitionsRightPageCounts[failedPartitionNum], OpType.SCAN);
            scanRight.setSchema(right.schema);
            return scanRight;
        }
    
        private void setupRecursiveHashJoin(PartitionScan scanLeft, PartitionScan scanRight) {
            Join join = setupJoin(scanLeft, scanRight, this.getCondition(), this.optype, schema,
                    this.getJoinType(), numBuff);
            recursiveJoin = new HashJoin(join, recursiveCount + 1);
            recursiveJoin.open();
        }
    
        private void setupBlockNestedJoin(PartitionScan scanLeft, PartitionScan scanRight) {
            Join join = setupJoin(scanLeft, scanRight, this.getCondition(), this.optype, schema,
                    JoinType.BLOCKNESTED, numBuff);
            recursiveJoin = new BlockNestedJoin(join);
            recursiveJoin.open();
        }
    
        private Join setupJoin(PartitionScan scanLeft, PartitionScan scanRight, Condition condition, int opType,
                               Schema schema, int joinType, int numBuff) {
            Join join = new Join(scanLeft, scanRight, condition, opType);
            join.setSchema(schema);
            join.setJoinType(joinType);
            join.setNumBuff(numBuff);
            return join;
        }
    
        private void closeOutputBuffer() {
            failedPartition.remove(0);
            recursiveJoin.close();
            recursiveJoin = null;
        }
    }

### PartitionScan.java <a name="#partition-scan"></a>
    public class PartitionScan extends Operator {
        String fileNamePrefix;  // corresponding file name
        Batch inputBuffer;
        int filePageCount;  // table name
        int pageCursor;
    
        public PartitionScan(String fileNamePrefix, int count, int type) {
            super(type);
            this.fileNamePrefix = fileNamePrefix;
            this.filePageCount = count;
        }
    
        // Opens the file to prepare a stream pointer to read input file */
        public boolean open() {
            pageCursor = 0;
            return true;
        }
    
        public Batch next() {
            inputBuffer = null;
            if (pageCursor < filePageCount) {
                pageRead(fileNamePrefix + pageCursor);
                pageCursor++;
            }
            return inputBuffer;
        }
    
        public boolean close() {
            inputBuffer = null;
            return true;
        }
    
        public Object clone() {
            PartitionScan partitionScan = new PartitionScan(fileNamePrefix, filePageCount, optype);
            partitionScan.setSchema((Schema) schema.clone());
            return partitionScan;
        }
    
        private void pageRead(String fileName) {
            ObjectInputStream inputStream = null;
            try {
                inputStream = new ObjectInputStream(new FileInputStream(fileName));
                System.out.println(fileName);
            } catch (IOException io) {
                System.err.println("Partition Scan: Error in reading the file === " + fileName);
            }
    
            try {
                inputBuffer = (Batch) inputStream.readObject();
                inputStream.close();
            } catch (EOFException e) {
                try {
                    inputStream.close();
                } catch (IOException io) {
                    System.err.println("Partition Scan: Error in temporary file reading === " + fileName);
                }
            } catch (ClassNotFoundException c) {
                System.err.println("Partition Scan: Some error in deserialization  === " + fileName);
                System.exit(1);
            } catch (IOException io) {
                System.err.println("Partition Scan: Temporary file reading error  === " + fileName);
                System.exit(1);
            }
        }
    }

### TupleIterator.java <a name="#tuple-iterator"></a>
    public class TupleIterator implements Iterator<Tuple> {
        String fileNamePrefix;
        int totalPageNum;
        int pageCursor;
    
        Batch buffer;
        Iterator<Tuple> iterator;
    
        public TupleIterator(String fileNamePrefix, int totalPageNum) {
            this.fileNamePrefix = fileNamePrefix;
            this.totalPageNum = totalPageNum;
            this.pageCursor = -1;
        }
    
        public boolean hasNext() {
            if (iterator == null || !iterator.hasNext()) {
                return loadNextPage();
            }
            return iterator.hasNext();
        }
    
        public Tuple next() {
            if (hasNext()) {
                return iterator.next();
            } else {
                return null;
            }
        }
    
        private boolean loadNextPage() {
            if (pageCursor + 1 >= totalPageNum) {
                return false;
            }
    
            if (!pageRead(fileNamePrefix + (pageCursor + 1))) {
                return false;
            }
    
            pageCursor++;
    
            if (buffer.isEmpty()) {
                return false;
            }
    
            iterator = buffer.getIterator();
            return iterator.hasNext();
        }
    
        /**
         * Read batch into input buffer.
         *
         * @param fileName name of the file to be read
         * @return True if page has been read into input buffer
         */
        private boolean pageRead(String fileName) {
            ObjectInputStream inputStream;
            try {
                inputStream = new ObjectInputStream(new FileInputStream(fileName));
            } catch (IOException io) {
                System.err.println("TupleIterator: Error in reading the file === " + fileName);
                return false;
            }
    
            try {
                buffer = (Batch) inputStream.readObject();
                inputStream.close();
            } catch (EOFException e) {
                try {
                    inputStream.close();
                } catch (IOException io) {
                    System.err.println("TupleIterator: Error in temporary file reading === " + fileName);
                }
            } catch (ClassNotFoundException c) {
                System.err.println("TupleIterator: Some error in deserialization  === " + fileName);
                System.exit(1);
            } catch (IOException io) {
                System.err.println("TupleIterator: Temporary file reading error  === " + fileName);
                System.exit(1);
            }
    
            return true;
        }
    }

## Optimizers <a name="#optmizers"></a>
### BufferManager.java <a name="#buffer-manager"></a>
    public BufferManager(int numBuffer, int numJoin) {
        // Others omitted...
        
        if (numJoin > 0) {
            buffPerJoin = numBuffer / numJoin;
        }
    }

### PlanCost.java <a name="#plan-cost"></a>
    /**
     * Calculates the statistics and cost of join operation
     **/
    protected long getStatistics(Join node) {
        // Others omitted...
        
        switch (joinType) {
            case JoinType.NESTEDJOIN:
                joincost = leftpages * rightpages;
                break;
            case JoinType.BLOCKNESTED:
                joincost = 0;
                break;
            case JoinType.HASHJOIN:
                joincost = 3 * (leftpages + rightpages);
                break;
            default:
                System.out.println("join type is not supported");
                joincost = 0;
                break;
        }
    }

### RandomInitialPlan.java <a name="#random-initial-plan"></a>
    public class RandomInitialPlan {
        // Others omitted...
        
        boolean isDistinct;
        
        public RandomInitialPlan(SQLQuery sqlquery) {
            // Others omitted...
            
            isDistinct = sqlquery.isDistinct();
        }
        
        /**
         * prepare initial plan for the query
         **/
        public Operator prepareInitialPlan() {
            if (sqlquery.getOrderByList().size() > 0) {
                System.err.println("Orderby is not implemented.");
                System.exit(1);
            }
    
            tab_op_hash = new HashMap<>();
            createScanOp();
            createSelectOp();
            if (numJoin != 0) {
                createJoinOp();
            }
            if (isDistinct) {
                createDistinctOp();
            }
            createProjectOp();
            createGroupbyOp();
            return root;
        }
        
        public void createGroupbyOp() {
            Operator base = root;
            if (groupbylist == null) {
                groupbylist = new ArrayList<>();
            }
    
            if (!groupbylist.isEmpty()) {
                root = new GroupBy(base, groupbylist, OpType.GROUPBY);
                Schema newSchema = base.getSchema().subSchema(groupbylist);
                root.setSchema(newSchema);
            }
        }
    
    
        private void createDistinctOp() {
            Operator base = root;
            if (isDistinct) {
                Schema newSchema;
                if (projectlist.isEmpty()) {
                    projectlist = base.getSchema().getAttList();
                    newSchema = base.getSchema();
                } else {
                    newSchema = base.getSchema().subSchema(projectlist);
                }
                root = new Distinct(base, projectlist, OpType.DISTINCT);
                root.setSchema(newSchema);
            }
        }
    }

### RandomOptimizer.java <a name="#random-optimizer"></a>
    public class RandomOptimizer {
        /**
         * After finding a choice of method for each operator
         * * prepare an execution plan by replacing the methods with
         * * corresponding join operator implementation
         **/
        public static Operator makeExecPlan(Operator node) {
            if (node.getOpType() == OpType.JOIN) {
                Operator left = makeExecPlan(((Join) node).getLeft());
                Operator right = makeExecPlan(((Join) node).getRight());
                int joinType = ((Join) node).getJoinType();
                int numbuff = BufferManager.getBuffersPerJoin();
                switch (joinType) {
                    case JoinType.NESTEDJOIN:
                        NestedJoin nj = new NestedJoin((Join) node);
                        nj.setLeft(left);
                        nj.setRight(right);
                        nj.setNumBuff(numbuff);
                        return nj;
                    case JoinType.BLOCKNESTED:
                        BlockNestedJoin bnj = new BlockNestedJoin((Join) node);
                        bnj.setLeft(left);
                        bnj.setRight(right);
                        bnj.setNumBuff(numbuff);
                        return bnj;
                    case JoinType.HASHJOIN:
                        HashJoin hj = new HashJoin((Join) node);
                        hj.setLeft(left);
                        hj.setRight(right);
                        hj.setNumBuff(numbuff);
                        return hj;
                    default:
                        return node;
                }
            } else if (node.getOpType() == OpType.GROUPBY) {
                Operator base = makeExecPlan(((GroupBy) node).getBase());
                ((GroupBy) node).setBase(base);
                return node;
            } else if (node.getOpType() == OpType.DISTINCT) {
                int numBuff = BufferManager.numBuffer;
                Operator base = makeExecPlan(((Distinct) node).getBase());
                ((Distinct) node).setBase(base);
                ((Distinct) node).setNumBuff(numBuff);
                return node;
            }
            
            // Others omitted...
        }
        
        /**
         * This method traverses through the query plan and
         * * returns the node mentioned by joinNum
         **/
        protected Operator findNodeAt(Operator node, int joinNum) {
            // Others omitted...
            
            else if (node.getOpType() == OpType.GROUPBY) {
                return findNodeAt(((GroupBy) node).getBase(), joinNum);
            } else if (node.getOpType() == OpType.DISTINCT) {
                return findNodeAt(((Distinct) node).getBase(), joinNum);
            } else {
                return null;
            }
        }
    }

## Utils <a name="#utils"></a>
### AttrComparator.java <a name="#attr-comparator"></a>
    public class AttrComparator implements Comparator<Tuple> {
        private int[] attrIndex;
    
        public AttrComparator(int[] attrIndex) {
            this.attrIndex = attrIndex;
        }
    
        @Override
        public int compare(Tuple t1, Tuple t2) {
            for (int index : attrIndex) {
                int result = Tuple.compareTuples(t1, t2, index, index);
                if (result != 0) {
                    return result;
                }
            }
            return 0;
        }
    }

### Batch.java <a name="#batch"></a>
    public class Batch implements Serializable {
        // other implementation...
        
        public Iterator<Tuple> getIterator() {
            return tuples.iterator();
        }
    }

### Block.java <a name="#block"></a>
    public class Block implements Serializable {
        int MAX_SIZE;
        int pageSize;
        Vector<Batch> batches;
        Vector<Tuple> tuples;
    
        public Block(int numPage, int pageSize) {
            this.MAX_SIZE = numPage;
            this.pageSize = pageSize;
            this.batches = new Vector<>(MAX_SIZE);
            this.tuples = new Vector<>(pageSize * MAX_SIZE);
        }
    
        public Vector<Tuple> getTuples() {
            return tuples;
        }
    
        public Tuple getSingleTuple(int index) {
            return tuples.elementAt(index);
        }
    
        public void setTuples(Vector<Tuple> tupleList) {
            Batch batch = new Batch(pageSize);
            for (Tuple tuple : tupleList) {
                if (batch.isFull()) {
                    batches.add(batch);
                    batch = new Batch(pageSize);
                }
                batch.add(tuple);
                this.tuples.add(tuple);
            }
            if (!batch.isEmpty()) {
                this.batches.add(batch);
            }
        }
    
        public Vector<Batch> getBatches() {
            return batches;
        }
    
        public Batch getSingleBatch(int index) {
            return batches.elementAt(index);
        }
    
        public void setBatches(Vector<Batch> batches) {
            this.batches = batches;
            for (Batch batch : batches) {
                for (int j = 0; j < batch.size(); j++) {
                    tuples.add(batch.get(j));
                }
            }
        }
    
        public void addBatch(Batch b) {
            if (!isFull()) {
                batches.add(b);
                for (int i = 0; i < b.size(); i++) {
                    tuples.add(b.get(i));
                }
            }
        }
    
        public int getBatchSize() {
            return batches.size();
        }
    
        public int getTupleSize() {
            return tuples.size();
        }
    
        public boolean isEmpty() {
            return batches.isEmpty();
        }
    
        public boolean isFull() {
            return batches.size() >= MAX_SIZE;
        }
    }

### RandNumb.java <a name="#rand-numb"></a>
    public class RandNumb {
        // other implementation...
        
        private static int[] PRIME = {53, 97, 193, 389, 769, 1543, 3079, 6151, 12289, 24593, 49157, 98317, 196613};
    
        public static int randPrime() {
            int index = randInt(0, PRIME.length - 1);
            return PRIME[index];
        }
    }
