#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>

#include "structs.h"
#include "auxFun.h"

int32_t next_prime(int32_t x) {
    if (x < 2) return 2;
    if (x == 3) return 5;
    int32_t retValue = (x % 2) ? x + 2 : x + 1; // Next odd number
    while (1) {
        if (retValue % 3 != 0) {
            int isPrime = 1;
            for (int32_t i = 5; i*i <= retValue; i += 6) {
                if (retValue % i == 0 || retValue % (i+2) == 0) {
                    isPrime = 0;
                    break;
                }
            }
            if (isPrime)
                return retValue;
        }
        retValue += 2;
    }
}

int32_t pow2(int32_t exp){
    return 1 << exp;
}

/* int main() {
    for (int i=0; i<20; i++)
        printf("%d : %d = %d\n", i, next_prime(i), next_prime3(i));

    return 0;
} */