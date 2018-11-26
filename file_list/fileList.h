#ifndef _FILELIST_
#define _FILELIST_

typedef struct fileList{
	char *filepath;
	struct fileList *next;
}fileList;

fileList *push_file(fileList *, char *);
fileList *pop_file(fileList *, char **);

#endif
