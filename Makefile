test_assign1: test_assign1_1.o dberror.o storage_mgr.o
	gcc test_assign1_1.o dberror.o storage_mgr.o -o test_assign1

test_assign1_1.o: test_assign1_1.c dberror.c storage_mgr.c
	gcc -c test_assign1_1.c storage_mgr.c dberror.c 

dberror.o: dberror.c dberror.h
	gcc -c dberror.c

storage_mgr.o: storage_mgr.h storage_mgr.c
	gcc -c storage_mgr.c

clean: 
	del *.o test_assign1



