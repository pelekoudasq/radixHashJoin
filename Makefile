CFLAGS	= -O3 -Wall -std=c99
JOIN	= join
DEB		= joindeb
OBJS	= test.o auxFun.o resultList.o join.o 

.PHONY : all clean
all: $(JOIN) $(DEB)

$(JOIN): $(OBJS)
$(DEB): $(OBJS)
	gcc test.c auxFun.c resultList.c join.c -D_debug_ -o $(DEB)
clean:
	rm -f $(OBJS) $(JOIN) $(DEB)
run: all
	/bin/time ./join > /dev/null