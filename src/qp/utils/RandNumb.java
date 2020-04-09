/**
 * functions to get some random numbers
 * useful in random optimizer
 **/

package qp.utils;

import java.lang.Math;

public class RandNumb {

    /** Get a random number between a and b **/
    public static int randInt(int a, int b) {
        return ((int) (Math.floor(Math.random() * (b - a + 1)) + a));
    }

    /** Coin flip **/
    public static boolean flipCoin() {
        if (Math.random() < 0.5)
            return true;
        else
            return false;
    }

    private static int[] PRIME = {53, 97, 193, 389, 769, 1543, 3079, 6151, 12289, 24593, 49157, 98317, 196613};

    public static int randPrime() {
        int index = randInt(0, PRIME.length - 1);
        return PRIME[index];
    }
}
