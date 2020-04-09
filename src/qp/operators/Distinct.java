package qp.operators;

import qp.utils.*;

import java.io.*;
import java.util.*;

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
