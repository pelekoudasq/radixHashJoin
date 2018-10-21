CFLAGS	= -g3 -Wall
JOIN	= join
OBJS	= test.o join.o
LDFLAGS = -g3 -pthread

.PHONY : all clean
all: $(JOIN)

$(JOIN): $(OBJS)

clean:
	rm -f $(OBJS) $(JOIN)
