package com.steventc.prime;

public class PrimeUtil {

    public static boolean isPrime(int potentialPrime) {

        //Perhaps faster ways to do this with some bitshift magic

        if (potentialPrime % 2==0){
            return false;
        }

        for(int i=3; i*i<=potentialPrime; i+=2) {
            if(potentialPrime % i ==0)
                return false;
        }

        return true;
    }
}
