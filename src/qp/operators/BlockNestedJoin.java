package qp.operators;

import qp.optimizer.BufferManager;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

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
