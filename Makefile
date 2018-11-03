CFLAGS	= -O3 -Wall -std=c99
JOIN	= join
OBJS	= test.o auxFun.o resultList.o join.o 

.PHONY : all clean
all: $(JOIN)

$(JOIN): $(OBJS) -lm

clean:
	rm -f $(OBJS) $(JOIN)
