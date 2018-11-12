#include <ctype.h>
#include <fcntl.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/errno.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "structs.h"
#include "auxFun.h"
#include "join.h"
#include "resultList.h"



typedef struct fileList{
	char *filepath;
	struct fileList *next;
}fileList;

fileList *push_file(fileList *head, char* filepath){
	
	fileList *newNode = malloc(sizeof(fileList));
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

typedef struct {
	uint64_t num_tuples;
	uint64_t num_columns;
	uint64_t *value;
}relList;

uint64_t read_number(int ch, int* delim){

	uint64_t num = 0;
	while( isdigit(ch) ){
		num = num*10 + ch - '0';
		ch = getchar();
	}
	if (delim)
		*delim = ch;
	return num;
}



void push_table(uint64_t x){

}

void read_relations(){

	char ch = getchar();
	while(ch != '|'){
		//push_table
		push_table(read_number(ch, &ch));
	}
}


void read_predicates(){

	char ch = getchar();
	char delim;
	while(ch != '|'){
		uint64_t table1 = read_number(ch, NULL); //teleiia
		char op;
		uint64_t column1 = read_number(getchar(), &op);
		uint64_t unknown = read_number(getchar(), &delim);
		if(delim == '.'){
			uint64_t table2 = unknown;
			uint64_t column2 = read_number(getchar(), &ch); //op einai eite & eite |
			//////////add to join tables and columns and operation
		} else {
			//////////op filter with unknown
		}
	}
}


void read_projections(){}



int main(int argc, char const *argv[]){
	
	char *lineptr = NULL;
	size_t n = 0;
	ssize_t lineSize;
	fileList *list = NULL;
	int listSize = 0;

	//get every filepath, push it to the list
	while ( (lineSize = getline(&lineptr, &n, stdin)) != -1 && strcmp(lineptr, "Done\n") != 0 ){
		lineptr[lineSize-1] = '\0';
		char *filepath = malloc(lineSize);
		strcpy(filepath, lineptr);
		list = push_file(list, filepath);
		listSize++;
	}

	if( lineptr != NULL )
		free(lineptr);

	relList *relations = malloc(sizeof(relList)*listSize);

	for (int i = listSize-1; i >= 0; i--){

		char *filepath;
		list = pop_file(list, &filepath);
		int fileDesc = open(filepath, O_RDONLY);
		free(filepath);
		read(fileDesc, &relations[i].num_tuples, sizeof(uint64_t));
		read(fileDesc, &relations[i].num_columns, sizeof(uint64_t));
		relations[i].value = mmap(NULL, relations[i].num_tuples*relations[i].num_columns*sizeof(uint64_t), PROT_READ, MAP_PRIVATE, fileDesc, 0);
		relations[i].value += 2;
		close(fileDesc);
	}

	for (int i = 0; i < listSize; ++i){
		//printf("%ld, %ld\n", relations[i].num_tuples, relations[i].num_columns);
		for (int j = 0; j < relations[i].num_tuples; ++j){
			for (int k = 0; k < relations[i].num_columns; ++k)
			{
				printf("%ld|", relations[i].value[k*relations[i].num_tuples+j]);
			}
			printf("\n");

		}
	}

	//parse batch
	//read_relations();
	//read_predicates();
	//read_projections();

	for (int i = 0; i < listSize; ++i){
		munmap(relations[i].value, relations[i].num_tuples*relations[i].num_columns*sizeof(uint64_t));
	}
	free(relations);

	return 0;
}
