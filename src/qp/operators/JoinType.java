/**
 * Enumeration of join algorithm types
 * Change this class depending on actual algorithms
 * you have implemented in your query processor
 **/

package qp.operators;

public class JoinType {

    public static final int NESTEDJOIN = 1;
    public static final int BLOCKNESTED = 2;
    public static final int HASHJOIN = 0;

    public static int numJoinTypes() {
        return 3;
    }
}
