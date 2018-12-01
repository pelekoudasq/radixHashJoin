#ifndef _FILELIST_
#define _FILELIST_

struct fileList{
	char *filepath;
	fileList *next;
};

fileList *push_file(fileList *, char *);
fileList *pop_file(fileList *, char **);

#endif
