#include <stdio.h>
#include <stdlib.h>
#include "sqlaccess.h"

int create(char *table_name) {
	struct Table table = createTable(table_name);

	return -1;
}


int insert(char *data, int size, char *table_name){
	// map table.csd file into memory
	struct Table table = openTable(table_name);	
	
	// create record from data
	struct Record record = createRecord(data);

	// insert record returning what page the record was inserted on
	struct Page page = insertRecord(record, table);

	// create a node from the record to place in B-Tree
	struct Node node = createNode(page, record);

	// insert node into B-Tree
	insertNode(node);
}

int deleteRecord(char *field, int size, char *table) {
	return -1;
}

int update(char *field, int size, char *value, char *table) {	
	return -1;
}

char * select(char *condition, char *table) {
	struct Table table = openTable(table);
	
	// search pages

	return NULL;
}


int commit(char *table_name, struct Table table) {
	commitTable(table_name, table);
	return 0;
}

int drop(char *table) {
	return -1;
}

// TO DO
int alter() {}



