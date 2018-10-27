#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>

#include "structs.h"
#include "auxFun.h"

int32_t ipow(int32_t base, int32_t exp){

	int32_t result = 1;
	while (exp){
		if (exp & 1)
			result *= base;
		exp >>= 1;
		base *= base;
	}
	return result;
}

int32_t pow2(int32_t exp){
	return 1 << exp;
}
