CC			= g++
CXXFLAGS	= -std=c++11 -O3 -Wall -Wextra -Wconversion
JOIN		= join
OBJS		= test.o aux_fun/auxFun.o result_list/resultList.o join.o query_handler/query.o

.PHONY: all clean
all: $(JOIN)

$(JOIN): $(OBJS)

clean:
	rm -f $(OBJS) $(JOIN)

run: all
	cat small/small.init small/small.work | (time --format="Time Elapsed: %E" ./join)
