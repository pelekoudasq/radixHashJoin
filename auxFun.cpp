#include "auxFun.h"

/* Get next prime for hashing */
size_t next_prime(size_t x) {
    if (x < 2) return 2;
    if (x == 3) return 5;
    size_t retValue = (x % 2) ? x + 2 : x + 1; // Next odd number
    while (true) {
        if (retValue % 3 != 0) {
            bool isPrime = true;
            for (size_t i = 5; i * i <= retValue; i += 6) {
                if (retValue % i == 0 || retValue % (i + 2) == 0) {
                    isPrime = false;
                    break;
                }
            }
            if (isPrime)
                return retValue;
        }
        retValue += 2;
    }
}

size_t pow2(size_t exp) {
    return (size_t) 1 << exp;
}

