CFLAGS	= -O3 -Wall -std=c99
JOIN	= join
DEB		= joindeb
OBJS	= test.o aux_fun/auxFun.o result_list/resultList.o join.o file_list/fileList.o query_handler/query.o

.PHONY : all clean
all: $(JOIN) $(DEB)

$(JOIN): $(OBJS)
$(DEB): $(OBJS)
	gcc -g test.c aux_fun/auxFun.c result_list/resultList.c join.c file_list/fileList.c query_handler/query.c -D_debug_ -o $(DEB)
clean:
	rm -f $(OBJS) $(JOIN) $(DEB)
run: all
	time --format="Time Elapsed: %E" ./joindeb > /dev/null