#include <stdio.h>
#include <stdlib.h>
#include "sqlaccess.h"

#define MAX_RESULT_SIZE 100



// DATA BUFFER

DataBuffer * dataBuffer;

DataBuffer * initializeDataBuffer() {
	dataBuffer = malloc(sizeof(dataBuffer));
	dataBuffer->length = 0;
	dataBuffer->tables = cfuhash_new_with_initial_size(MAX_TABLE_SIZE); 
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
	
	// check if there is enough room for the new record
	// is there enough room on the page to insert record
        if((page->space_available - record->size_of_record) <= 0) {
		printf("\nCREATING NEW PAGE\n");
		page = createPage(table); // create a new page
	}
               
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
		free(indexKey);
	}

	free(recordKey);
		
	return 0;
}


int deleteRecord(char *database_name, char *table_name, char *condition_column_name, char *condition_value) {

	// overall we want to find the page and slot number of where the record is located
	Table *table = (Table *) cfuhash_get(dataBuffer->tables, table_name);
	Index *index;
	RecordKey *recordKey = NULL;

	printf("\nIn delete record\n");

	// check if the condition column is the rid or an index for quicker search, else perform a sequential search	
	// what is returned in each case is the page_number and slot_number of the record
	if(strcmp(condition_column_name, "rid") == 0) {
		recordKey = findRecordKey(table, atoi(condition_value));
	} else if((index = hasIndex(condition_column_name, table)) != NULL) {
		IndexKey *indexKey = findIndexKey(index, condition_value);
		if(indexKey != NULL) 
			recordKey = findRecordKey(table, indexKey->value);
		free(indexKey);
	} else {
		Record *record = sequentialSearch(condition_column_name, condition_value, table);
		if(record != NULL) 
			recordKey = findRecordKey(table, record->rid);	
	}
	
	// if we have found a match for our delete query, then delete that row from the table
	if(recordKey != NULL) {
		deleteRow(table, recordKey->value->page_number, recordKey->value->slot_number);
		free(recordKey);
		return 0;
	}


	printf("\nreturing -1\n");
	// else no match was found so return -1
	return -1;	
}



int update(char *field, int size, char *value, char *table) {	
	return -1;
}


// returns target column data
char * selectRecord(char *database_name, char *table_name, char *target_column_name, char *condition_column_name, char *condition_value) {
	
	if(cfuhash_exists(dataBuffer->tables, table_name)){
		// Table *table = openTable(table_name, database_name);
	}
	
	Table *table = (Table *) cfuhash_get(dataBuffer->tables, table_name);
	
	Record *record = searchRecord(table, condition_column_name, condition_value);
	
	// if record does not exist
	if(record == NULL) {
		printf("\nreturning null\n");
		return NULL;
	}
	
	// get the target column data
	char *buffer = malloc(MAX_RESULT_SIZE);
	if(getColumnData(record, target_column_name, buffer, table->format) == 0)
		return buffer;
	
	free(buffer);
	return NULL;
}


int commit(char *table_name, char *database_name) {
	Table *table = cfuhash_get(dataBuffer->tables, table_name);

	createFolder(database_name);

        // get paths to each file related to table
        char path_to_table[50];
        getPathToFile(".csd", table_name, database_name, path_to_table);

        char path_to_index[50];
        getPathToFile(".csi", table_name, database_name, path_to_index);

        char path_to_format[50];
        getPathToFile(".csf", table_name, database_name, path_to_format);

        // declare file streams for each file
        FILE *tp, *ip, *fp;

        // set the mode based on whether the table already exists
        char mode[3];
        if(fileExists(path_to_table) == -1)
                strcpy(mode, "rb+");
        else
                strcpy(mode, "wb+");

        // connect the file streams to each file
        tp = fopen(path_to_table, mode);
        ip = fopen(path_to_index, mode);
        fp = fopen(path_to_format, mode);

	// commit entire table
        /*
	commitIndexes(table->indexes->indexes[0], ip);
        commitFormat(table->format, fp);
	commitTable(table, tp);
	*/
	
	return 0;
}

int drop(char *table_name) {

	if(cfuhash_exists(dataBuffer->tables, table_name)){		
		// get table
		Table *table = (Table *) cfuhash_get(dataBuffer->tables, table_name);
		
		// free table (and all its entries)
		deleteTable(table);

		// remove table from dataBuffer
		cfuhash_delete(dataBuffer->tables, table_name);
		
		return 0;

	} else {

		return -1;

	}
}

// TO DO
int alterRecord(char *database_name, char *table_name, char *target_column_name, char *target_column_value, char *condition_column_name, char *condition_value) {

	// overall we want to find the page and slot number of where the record is located
        Table *table = (Table *) cfuhash_get(dataBuffer->tables, table_name);
        Index *index;
	Record *record;
        RecordKey *recordKey = NULL;

        // check if the condition column is the rid or an index for quicker search, else perform a sequential search
        // what is returned in each case is the page_number and slot_number of the record
        if(strcmp(condition_column_name, "rid") == 0) {
                recordKey = findRecordKey(table, atoi(condition_value));
        } else if((index = hasIndex(condition_column_name, table)) != NULL) {
                IndexKey *indexKey = findIndexKey(index, condition_value);
                if(indexKey != NULL)
                        recordKey = findRecordKey(table, indexKey->value);
                free(indexKey);
        } else {
                record = sequentialSearch(condition_column_name, condition_value, table);
                if(record != NULL)
                        recordKey = findRecordKey(table, record->rid);
        }

        // if we have found a match for our delete query, then delete that row from the table
        if(recordKey != NULL) {
                record = table->pages[recordKey->value->page_number]->records[recordKey->value->slot_number];
                free(recordKey);
        	
		// get the target column data
        	int i;
		for(i = 0; i < table->format->number_of_fields; ++i) {
			if(strcmp(table->format->fields[i]->name, target_column_name) == 0) {
				strcpy(record->data[i], target_column_value);
				return 0;
			}
		}
	}

	return -1;	
}


int alterTableAddColumn(char *database_name, char *table_name, char *column_name, char *data_type) {
	// get table
	Table *table = (Table *) cfuhash_get(dataBuffer->tables, table_name);

	// create new field setting field and type
	createField(data_type, column_name, table->format);
}


int alterTableDeleteColumn(char *database_name, char *table_name, char *column_name) {

	// get table
	Table *table = (Table *) cfuhash_get(dataBuffer->tables, table_name);

	// delete field
		
	
	// delete column data

	// shift deleted field + 1 down

	// shift record->data +1 down
	
}


int alterTableColumn(char *database_name, char *table_name, char *target_column, char *new_name) {
	Table *table = (Table *) cfuhash_get(dataBuffer->tables, table_name);

	int i;
	for(i = 0; i < table->format->number_of_fields; ++i) {
		if(strcmp(table->format->fields[i]->name, target_column) == 0) {
			strcpy(table->format->fields[i]->name, new_name);
			return 0;
		}
	} 

	return -1;
}

