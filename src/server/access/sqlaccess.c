#include <stdio.h>
#include <stdlib.h>
#include "sqlaccess.h"


#define MAX_TABLE_AMOUNT 30


// DATA BUFFER

DataBuffer * dataBuffer;

DataBuffer * initializeDataBuffer() {
	dataBuffer = malloc(sizeof(dataBuffer));
	dataBuffer->length = 0;
	dataBuffer->tables = cfuhash_new_with_initial_size(MAX_TABLE_AMOUNT); 
	cfuhash_set_flag(dataBuffer->tables, CFUHASH_FROZEN_UNTIL_GROWS);
	return dataBuffer;
}


//returns -1 if no room left in the table
int addTableToBuffer(char *table_name, Table *table) {
	if(dataBuffer->length < MAX_TABLE_SIZE) {
		cfuhash_put(dataBuffer->tables, table_name, table);
		dataBuffer->length++;
		return 0;
	} else {
		return -1;
	}
}

// CREATE DATABASE
int createDatabase(char *name) {
	createFolder(name);
}

int deleteDatabase(char *name){
	int result = deleteFolder(name);
}


// TABLE
int create(char *table_name, char *fields[], int number_of_fields) {
	Table *table = createTable(table_name);
	createIndexes(table);
	createFormat(table, fields, number_of_fields);
	addTableToBuffer(table_name, table);	
	
	return 0;
}


int insert(char *data[], int size, char *table_name, char *database_name) {

	Table *table;
	
	// if table is not in memory
	if(!cfuhash_exists(dataBuffer->tables, table_name)){
		// add table to memory
		//table = openTable(table_name, database_name);
		addTableToBuffer(table_name, table);
	}
	
	// get table from memory
	table = cfuhash_get(dataBuffer->tables, table_name);

	
	// RECORD		
	// create record from data
	Record *record = createRecord(data, table->format->number_of_fields, size);

	// get last page to insert record
	Page *page = table->pages[table->number_of_pages - 1];

	// insert record into table 
	insertRecord(record, page, table);

	// create a table record key from the record to place in a B-Tree node
	RecordKey *recordKey = createRecordKey(record->rid, page->number, page->number_of_records - 1);

	// insert node into table B-Tree
	insertRecordKey(recordKey, table);


	// INDEXES
	// get set of indexes associated with table
	Index **indexes = table->indexes->indexes;

	// for every index of the table (excluding primary index)
	int i;
	char buffer[100];
	for(i = 0; i < table->indexes->number_of_indexes; ++i) {
		// fetch the data located underneath that column
		getColumnData(record, indexes[i]->index_name, buffer, table->format);			
		// create index key (from newly inserted record)
		IndexKey *indexKey = createIndexKey(buffer, record->rid);	
		// insert index key into index
		insertIndexKey(indexKey, indexes[i]);	
	}
		
	return 0;
}

int deleteRecord(char *field, int size, char *table) {
	return -1;
}

int update(char *field, int size, char *value, char *table) {	
	return -1;
}

char * selectRecord(char *condition, char *table_name, char *database_name) {
	// Table *table = openTable(table_name, database_name);
	
	// search pages

	return NULL;
}


int commit(char *table_name, char *database_name) {
	Table *table = cfuhash_get(dataBuffer->tables, table_name);
	commitTable(table_name, table, database_name);
	return 0;
}

int drop(char *table) {
	return -1;
}

// TO DO
int alter() {}



