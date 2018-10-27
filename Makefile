CFLAGS	= -g3 -Wall -std=c99 -lm
JOIN	= join
OBJS	= test.o join.o auxFun.o
LDFLAGS = -g3 

.PHONY : all clean
all: $(JOIN)

$(JOIN): $(OBJS) -lm

clean:
	rm -f $(OBJS) $(JOIN)
