#include <stdio.h>
#include <stdlib.h>
#include "sqlaccess.h"
#include "../../libs/libcfu/src/cfuhash.h"

#define MAX_TABLE_AMOUNT 30


// DATA BUFFER
// holds already loaded tables
struct DataBuffer {
	int length;
	cfuhash_table_t *tables;
}

struct DataBuffer dataBuffer;

void intitializeDataBuffer() {
	dataBuffer.length = 0;
	arguments = cfuhash_new_with_initial_size(MAX_TABLE_AMOUNT); 
	cfuhash_set_flag(arguments, CFUHASH_FROZEN_UNTIL_GROWS);
}


//returns -1 if no room left in the table
int addTableToBuffer(char *table_name, struct Table table) {
	if(dataBuffer.length < MAX_TABLE_SIZE) {
		cfuhash_put(dataBuffer.tables, table_name, &table);
		dataBuffer.length++;
		return 0;
	} else {
		return -1;
	}
}

// CREATE DATABASE
int createDatabase(char *name) {
	createFolder(name);
}


// TABLE
int create(char *table_name) {
	struct Table table = createTable(table_name);
	addTableToBuffer(table_name, table);	
	
	return -1;
}


int insert(char *data, int size, char *table_name, char *database_name){

	struct Table table;
	
	// if table is in memory
	if(cfuhash_exists(tables, table_name))
		table = cfuhash_get(dataBuffer.tables, table_name);
	else // map table from disk into memory 
		table = openTable(table_name, database_name);
	
	// create record from data
	struct Record record = createRecord(data);

	// insert record returning what page the record was inserted on
	struct Page page = insertRecord(record, table);

	// create a node from the record to place in B-Tree
	struct Node node = createNode(page, record);

	// insert node into B-Tree
	insertNode(node);

	// add table to buffer
	addTableToBuffer(table_name, table);

	return 0;
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


int commit(char *table_name, struct Table table, char *database_name) {
	commitTable(table_name, table, database_name);
	return 0;
}

int drop(char *table) {
	return -1;
}

// TO DO
int alter() {}



