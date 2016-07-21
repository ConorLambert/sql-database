#include <stdio.h>
#include <stdlib.h>
#include "sqlaccess.h"

#define MAX_TABLE_AMOUNT 30



// data buffer for already loaded tables
// array of tables indexed by table_name
// TO DO use a hash table
struct DataBuffer {
	int length;
	struct Table *tables[MAX_TABLE_AMOUNT];
}

struct DataBuffer dataBuffer;

void intitializeDataBuffer() {
	dataBuffer.length = 0

}

// TO DO use hashing algorithm
/*
	returns -1 if no room left in the table
*/
int addTableToBuffer(struct Table table) {
	// ... (hash table)
	dataBuffer.length++;
	return -1;
}


/*
	checks the data buffer to see if the table is already in memory
	returns -1 if table not in data buffer
	returns index position of table if table is in buffer
*/
int indexOfTable(char *table_name) {
	// TO DO 
	// use hashing algorithm on table_name to get index position
	int index = -1;

	return index;
}


int create(char *table_name) {
	struct Table table = createTable(table_name);
	addTableToBuffer(table);	
	
	return -1;
}


int insert(char *data, int size, char *table_name){

	struct Table table;
	
	int index = 0;
	// if table is in memory
	if((index = indexOfTable(table_name)) >= 0 )
		table = dataBuffer.tables[index];
	else // map table from disk into memory 
		table = openTable(table_name);
	
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



