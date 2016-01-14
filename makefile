.SUFFIXES : .c .o

CC = g++

CFALGS = -g -Wall -ansi
 all: hw

OBJS = ./src/main.o
SRCS = ./src/$(OBJS: .o=.c)
BIN  = ./bin/
LIB  = ./lib
INCLUDE = ./include

CFLAGS += -I$(INCLUDE)

hw:	$(OBJS)
	$(CC) -o  $(BIN)homework $(OBJS) -L$(LIB) -lboost_system -lboost_thread -lrt -pthread

clean:
	rm -rf $(OBJS)

allclean:
	rm -rf $(OBJS) $(BIN)*
