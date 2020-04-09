package qp.operators;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import qp.utils.*;

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
