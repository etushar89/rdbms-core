CC=gcc
CFLAGS=-pthread -O3 -m64 -c -Wall -fmessage-length=0

all: assign_2

dberror.o: dberror.c
	$(CC) $(CFLAGS) dberror.c

storage_mgr.o: storage_mgr.c
	$(CC) $(CFLAGS) storage_mgr.c

buffer_mgr_page_op.o: buffer_mgr_page_op.c
	$(CC) $(CFLAGS) buffer_mgr_page_op.c

buffer_mgr_pool_op.o: buffer_mgr_pool_op.c
	$(CC) $(CFLAGS) buffer_mgr_pool_op.c

buffer_mgr_stat.o: buffer_mgr_stat.c
	$(CC) $(CFLAGS) buffer_mgr_stat.c

test_assign2_1.o: test_assign2_1.c
	$(CC) $(CFLAGS) test_assign2_1.c

assign_2: dberror.o storage_mgr.o buffer_mgr_page_op.o buffer_mgr_pool_op.o buffer_mgr_stat.o test_assign2_1.o
	$(CC) dberror.o storage_mgr.o buffer_mgr_page_op.o buffer_mgr_pool_op.o buffer_mgr_stat.o test_assign2_1.o -o assign2

clean:
	rm *.o assign2