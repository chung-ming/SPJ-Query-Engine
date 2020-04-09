package qp.operators;

import qp.utils.*;

import java.io.*;

// The Scan Operator reads data from a file
// The PartitionScan scans the base relational table
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
