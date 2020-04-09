package qp.operators;

import qp.utils.*;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.*;

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


    // Closes the operator
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
