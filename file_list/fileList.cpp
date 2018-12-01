#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <cstdint>

#include "fileList.h"


fileList *push_file(fileList *head, char *filepath){

	fileList *newNode = (fileList*)malloc(sizeof(fileList));
	newNode->filepath = filepath;
	newNode->next = head;
	return newNode;
}

fileList *pop_file(fileList *head, char **filepath){

	fileList *temp = head->next;
	*filepath = head->filepath;
	free(head);
	return temp;
}
