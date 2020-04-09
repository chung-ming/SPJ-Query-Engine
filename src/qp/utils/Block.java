/**
 * Block represents a number of pages
 **/

package qp.utils;

import java.io.Serializable;
import java.util.Vector;

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
