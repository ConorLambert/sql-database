#include <stdio.h>
#include <stdlib.h>
#include "sqlaccess.h"
#include "../../../libs/libcfu/src/cfuhash.h"

#define MAX_TABLE_AMOUNT 30


// DATA BUFFER
// holds already loaded tables
struct DataBuffer {
	int length;
	cfuhash_table_t *tables;
};

struct DataBuffer dataBuffer;

void intitializeDataBuffer() {
	dataBuffer.length = 0;
	dataBuffer.tables = cfuhash_new_with_initial_size(MAX_TABLE_AMOUNT); 
	cfuhash_set_flag(dataBuffer.tables, CFUHASH_FROZEN_UNTIL_GROWS);
}


//returns -1 if no room left in the table
int addTableToBuffer(char *table_name, Table *table) {
	if(dataBuffer.length < MAX_TABLE_SIZE) {
		cfuhash_put(dataBuffer.tables, table_name, table);
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
	Table *table = createTable(table_name);
	addTableToBuffer(table_name, table);	
	
	return 0;
}


int insert(char *data, int size, char *table_name, char *database_name) {

	Table *table;
	
	// if table is not in memory
	if(!cfuhash_exists(dataBuffer.tables, table_name)){
		table = openTable(table_name, database_name);
		addTableToBuffer(table_name, table);
	}
	
	table = cfuhash_get(dataBuffer.tables, table_name);
		
	// create record from data
	Record *record = createRecord(data);

	// get last page to insert record
	Page *page = table->pages[table->number_of_pages - 1];

	// insert record returning the rid of the record
	insertRecord(record, page, table);

	// create a node from the record to place in B-Tree
	RecordNode recordNode = createRecordNode(record->rid, page->number, page->number_of_records - 1);

	// insert node into B-Tree
	insertRecordNode(&recordNode);

	return 0;
}

int deleteRecord(char *field, int size, char *table) {
	return -1;
}

int update(char *field, int size, char *value, char *table) {	
	return -1;
}

char * selectRecord(char *condition, char *table_name, char *database_name) {
	Table *table = openTable(table_name, database_name);
	
	// search pages

	return NULL;
}


int commit(char *table_name, char *database_name) {
	Table *table = cfuhash_get(dataBuffer.tables, table_name);
	commitTable(table_name, table, database_name);
	return 0;
}

int drop(char *table) {
	return -1;
}

// TO DO
int alter() {}



