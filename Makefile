CC		= g++
CFLAGS	= -O3 -Wall
JOIN	= join
DEB		= joindeb
OBJS	= test.o aux_fun/auxFun.o result_list/resultList.o join.o file_list/fileList.o query_handler/query.o

.PHONY : all clean
all: $(JOIN) $(DEB)

$(JOIN): $(OBJS)
$(DEB): $(OBJS)
	g++ -g test.cpp aux_fun/auxFun.cpp result_list/resultList.cpp join.cpp file_list/fileList.cpp query_handler/query.cpp -D_debug_ -o $(DEB)
clean:
	rm -f $(OBJS) $(JOIN) $(DEB)
run: all
	time --format="Time Elapsed: %E" ./joindeb > /dev/null