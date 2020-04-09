package qp.operators;

import qp.utils.*;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Iterator;

// To project out the group by attributes from the result
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
