test_assign3_1: test_assign3_1.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o dberror.o expr.o record_mgr.o rm_serializer.o 
	gcc -o test_assign3_1 test_assign3_1.o dberror.o storage_mgr.o buffer_mgr.o -lm buffer_mgr_stat.o  expr.o record_mgr.o rm_serializer.o 

test_expr: test_expr.o dberror.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o expr.o record_mgr.o rm_serializer.o 
	gcc -o  test_expr storage_mgr.o buffer_mgr.o -lm buffer_mgr_stat.o test_expr.o dberror.o expr.o record_mgr.o rm_serializer.o 

test_assign3_1.o: test_assign3_1.c dberror.h storage_mgr.h test_helper.h buffer_mgr.h buffer_mgr_stat.h
	gcc -c test_assign3_1.c

test_expr.o: test_expr.c dberror.h expr.h record_mgr.h test_helper.h tables.h 
	gcc -c test_expr.c 

record_mgr.o:storage_mgr.h buffer_mgr.h record_mgr.c record_mgr.h  
	gcc -c  record_mgr.c

expr.o: expr.c dberror.h record_mgr.h expr.h tables.h
	gcc -c expr.c

rm_serializer.o: rm_serializer.c dberror.h tables.h record_mgr.h
	gcc -c rm_serializer.c

buffer_mgr_stat.o: buffer_mgr_stat.h buffer_mgr_stat.c buffer_mgr.h
	gcc -c buffer_mgr_stat.c

buffer_mgr.o: buffer_mgr.h buffer_mgr.c storage_mgr.h dt.h
	gcc -c buffer_mgr.c

dberror.o: dberror.c dberror.h
	gcc -c dberror.c

storage_mgr.o: storage_mgr.h storage_mgr.c
	gcc -c storage_mgr.c
clean: 
	rm *.o