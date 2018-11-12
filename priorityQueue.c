#include <stdlib.h>
#include <stdio.h>

#include "priorityQueue.h"

void Queue_create(Queue *oura, int size){

	oura->embros = 0;
	oura->piso = 0;
	oura->size = size;
	oura->joinssServed = 0;
	oura->bytesReturned = 0;
	oura->buffer = malloc(size * sizeof(QueueType));
}
