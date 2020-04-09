/**
 * This method calculates the cost of the generated plans
 * also estimates the statistics of the result relation
 **/

package qp.optimizer;

import qp.operators.*;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Schema;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

public class PlanCost {

    long cost;
    long numtuple;

    /**
     * If buffers are not enough for a selected join
     * * then this plan is not feasible and return
     * * a cost of infinity
     **/
    boolean isFeasible;

    // hashMap stores mapping from Attribute name to number of distinct values of that attribute
    HashMap<Attribute, Long> hashMap;


    public PlanCost() {
        hashMap = new HashMap<>();
        cost = 0;
    }

    /**
     * Returns the cost of the plan
     **/
    public long getCost(Operator root) {
        cost = 0;
        isFeasible = true;
        numtuple = calculateCost(root);
        if (isFeasible) {
            return cost;
        } else {
            System.out.println("notFeasible");
            return Long.MAX_VALUE;
        }
    }

    /**
     * Get number of tuples in estimated results
     **/
    public long getNumTuples() {
        return numtuple;
    }


    /**
     * Returns number of tuples in the root
     **/
    protected long calculateCost(Operator node) {
        if (node.getOpType() == OpType.JOIN) {
            return getStatistics((Join) node);
        } else if (node.getOpType() == OpType.SELECT) {
            return getStatistics((Select) node);
        } else if (node.getOpType() == OpType.PROJECT) {
            return getStatistics((Project) node);
        } else if (node.getOpType() == OpType.SCAN) {
            return getStatistics((Scan) node);
        } else if (node.getOpType() == OpType.GROUPBY) {
            return getStatistics((GroupBy) node);
        } else if (node.getOpType() == OpType.DISTINCT) {
            return getStatistics((Distinct)node);
        }
        System.out.println("operator is not supported");
        isFeasible = false;
        return -1;
    }

    protected long getStatistics(GroupBy node) {
        return calculateCost(node.getBase());
    }

    protected long getStatistics(Distinct node) {
        return calculateCost(node.getBase());
    }

    /**
     * Projection will not change any statistics
     * * No cost involved as done on the fly
     **/
    protected long getStatistics(Project node) {
        return calculateCost(node.getBase());
    }

    /**
     * Calculates the statistics and cost of join operation
     **/
    protected long getStatistics(Join node) {
        long lefttuples = calculateCost(node.getLeft());
        long righttuples = calculateCost(node.getRight());

        if (!isFeasible) {
            return -1;
        }

        Condition condition = node.getCondition();
        Schema leftschema = node.getLeft().getSchema();
        Schema rightschema = node.getRight().getSchema();

        /** Get size of the tuple in output & correspondigly calculate
         ** buffer capacity, i.e., number of tuples per page **/
        long tuplesize = node.getSchema().getTupleSize();
        long outcapacity = Batch.getPageSize() / tuplesize;
        long leftuplesize = leftschema.getTupleSize();
        long leftcapacity = Batch.getPageSize() / leftuplesize;
        long righttuplesize = rightschema.getTupleSize();
        long rightcapacity = Batch.getPageSize() / righttuplesize;
        long leftpages = (long) Math.ceil(((double) lefttuples) / (double) leftcapacity);
        long rightpages = (long) Math.ceil(((double) righttuples) / (double) rightcapacity);


        Attribute leftjoinAttr = condition.getLhs();
        Attribute rightjoinAttr = (Attribute) condition.getRhs();
        int leftattrind = leftschema.indexOf(leftjoinAttr);
        int rightattrind = rightschema.indexOf(rightjoinAttr);
        leftjoinAttr = leftschema.getAttribute(leftattrind);
        rightjoinAttr = rightschema.getAttribute(rightattrind);

        /** Number of distinct values of left and right join attribute **/
        long leftattrdistn = hashMap.get(leftjoinAttr);
        long rightattrdistn = hashMap.get(rightjoinAttr);

        int outtuples = (int) Math.ceil(((double) lefttuples * righttuples) /
                (double) Math.max(leftattrdistn,rightattrdistn));

        long mindistinct = Math.min(leftattrdistn, rightattrdistn);
        hashMap.put(leftjoinAttr, mindistinct);
        hashMap.put(rightjoinAttr, mindistinct);

        /** Calculate the cost of the operation **/
        int joinType = node.getJoinType();
        long numbuff = BufferManager.getBuffersPerJoin();
        long joincost;

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
        cost = cost + joincost;

        return outtuples;
    }

    /**
     * Find number of incoming tuples, Using the selectivity find # of output tuples
     * * And statistics about the attributes
     * * Selection is performed on the fly, so no cost involved
     **/
    protected long getStatistics(Select node) {
        long intuples = calculateCost(node.getBase());
        if (!isFeasible) {
            System.out.println("notFeasible");
            return Long.MAX_VALUE;
        }

        Condition con = node.getCondition();
        Schema schema = node.getSchema();
        Attribute attr = con.getLhs();
        int index = schema.indexOf(attr);
        Attribute fullattr = schema.getAttribute(index);
        int exprtype = con.getExprType();

        /** Get number of distinct values of selection attributes **/
        long numdistinct = intuples;
        Long temp = hashMap.get(fullattr);
        numdistinct = temp.longValue();

        long outtuples;
        /** Calculate the number of tuples in result **/
        if (exprtype == Condition.EQUAL) {
            outtuples = (long) Math.ceil((double) intuples / (double) numdistinct);
        } else if (exprtype == Condition.NOTEQUAL) {
            outtuples = (long) Math.ceil(intuples - ((double) intuples / (double) numdistinct));
        } else {
            outtuples = (long) Math.ceil(0.5 * intuples);
        }

        /** Modify the number of distinct values of each attribute
         ** Assuming the values are distributed uniformly along entire
         ** relation
         **/
        for (int i = 0; i < schema.getNumCols(); ++i) {
            Attribute attri = schema.getAttribute(i);
            long oldvalue = hashMap.get(attri);
            long newvalue = (long) Math.ceil(((double) outtuples / (double) intuples) * oldvalue);
            hashMap.put(attri, outtuples);
        }
        return outtuples;
    }

    /**
     * The statistics file <tablename>.stat to find the statistics
     * * about that table;
     * * This table contains number of tuples in the table
     * * number of distinct values of each attribute
     **/
    protected long getStatistics(Scan node) {
        String tablename = node.getTabName();
        String filename = tablename + ".stat";
        Schema schema = node.getSchema();
        int numAttr = schema.getNumCols();
        BufferedReader in = null;
        try {
            in = new BufferedReader(new FileReader(filename));
        } catch (IOException io) {
            System.out.println("Error in opening file" + filename);
            System.exit(1);
        }
        String line = null;

        // First line = number of tuples
        try {
            line = in.readLine();
        } catch (IOException io) {
            System.out.println("Error in readin first line of " + filename);
            System.exit(1);
        }
        StringTokenizer tokenizer = new StringTokenizer(line);
        if (tokenizer.countTokens() != 1) {
            System.out.println("incorrect format of statastics file " + filename);
            System.exit(1);
        }
        String temp = tokenizer.nextToken();
        long numtuples = Long.parseLong(temp);
        try {
            line = in.readLine();
        } catch (IOException io) {
            System.out.println("error in reading second line of " + filename);
            System.exit(1);
        }
        tokenizer = new StringTokenizer(line);
        if (tokenizer.countTokens() != numAttr) {
            System.out.println("incorrect format of statistics file " + filename);
            System.exit(1);
        }
        for (int i = 0; i < numAttr; ++i) {
            Attribute attr = schema.getAttribute(i);
            temp = tokenizer.nextToken();
            Long distinctValues = Long.valueOf(temp);
            hashMap.put(attr, distinctValues);
        }

        /** Number of tuples per page**/
        long tuplesize = schema.getTupleSize();
        long pagesize = Math.max(Batch.getPageSize() / tuplesize, 1);
        long numpages = (long) Math.ceil((double) numtuples / (double) pagesize);

        cost = cost + numpages;

        try {
            in.close();
        } catch (IOException io) {
            System.out.println("error in closing the file " + filename);
            System.exit(1);
        }
        return numtuples;
    }

}
