CFLAGS	= -g3 -Wall -std=c99
JOIN	= join
OBJS	= test.o join.o auxFun.o
LDFLAGS = -g3 -pthread

.PHONY : all clean
all: $(JOIN)

$(JOIN): $(OBJS)

clean:
	rm -f $(OBJS) $(JOIN)
