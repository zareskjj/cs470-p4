CC=mpicc
CFLAGS=-g -O2 --std=c99 -Wall
LDFLAGS=-g -O2 -lpthread

OBJS=main.o dht.o local.o
EXE=dht

$(EXE): $(OBJS)
	$(CC) -o $@ $^ $(LDFLAGS)

%.o: %.c
	$(CC) $(CFLAGS) -c $<

clean:
	rm -f $(OBJS) $(EXE) dump*.txt

run:
	salloc -n 4 mpirun ./dht input.txt
