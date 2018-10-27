#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <math.h>

#include "structs.h"
#include "auxFun.h"

int32_t nextPrime(int32_t x) {
    int32_t retValue = x + 1;
    if(x % 2)
        retValue = x + 2;
    int flag;
    while(1) {
        flag = 0;
        for(int i = 2; i < sqrt(retValue); i++){
            if(!(retValue % i)){
                flag++;
                break;
            }
        }
        if (!flag)
            return retValue;
        retValue += 2;
    }
}

int32_t pow2(int32_t exp){
	return 1 << exp;
}
