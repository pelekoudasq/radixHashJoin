#ifndef _QUEUE_
#define _QUEUE_

typedef struct join {
	uint64_t table1;
	uint64_t column1;
	uint64_t table2;
	uint64_t column2;
	char operation;
} QueueType;

typedef struct {
	int embros;
	int piso;
	int size;
	int joinsServed;
	int bytesReturned;
	QueueType* buffer;
} Queue;

#endif
