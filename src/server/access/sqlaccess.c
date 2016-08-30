#include <stdio.h>
#include <stdlib.h>
#include "sqlaccess.h"

#define MAX_RESULT_SIZE 100
#define MAX_RESULT_ROW_SIZE 100


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

int addConstraintForeignKey(char *target_table_name, char *origin_table_name, char *field) {
	Table *target_table = (Table *) cfuhash_get(dataBuffer->tables, target_table_name);	
	Table *origin_table = (Table *) cfuhash_get(dataBuffer->tables, origin_table_name);

	int pos = locateField(origin_table->format, field);
	
	addForeignKey(target_table, origin_table, origin_table->format->fields[pos]);
}


int insert(char **data, int size, char *table_name, char *database_name) {

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
	
printf("\n returning 0\n");
	return 0;
}


int deleteRecord(char *database_name, char *table_name, char *condition_column_name, char *condition_value) {

	// overall we want to find the page and slot number of where the record is located
	Table *table = (Table *) cfuhash_get(dataBuffer->tables, table_name);
	Index *index;
	RecordKey *recordKey = NULL;
	Record *record;

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
		record = sequentialSearch(condition_column_name, condition_value, table, 0, 0);
		if(record != NULL) 
			recordKey = findRecordKey(table, record->rid);			
	}
	
	// if we have found a match for our delete query, then delete that row from the table
	if(recordKey != NULL) {
		deleteRow(table, recordKey->value->page_number, recordKey->value->slot_number);
		free(recordKey);
		return 0;
	}

	
	// TO DO
	// move the record_position of the page to the preceeding record


	// else no match was found so return -1
	return -1;	
}



int update(char *field, int size, char *value, char *table) {	
	return -1;
}


// returns target column data
char * selectRecord(char *database_name, char *table_name, char *target_column_name, char *condition_column_name, char *condition_value) {
	
	Table *table = (Table *) cfuhash_get(dataBuffer->tables, table_name);
        Index *index = hasIndex(condition_column_name, table);
	Record *record = NULL;
        RecordKey *recordKey = NULL;

	// result set variables
	char **result = malloc(table->rid * sizeof(char *));
	int k = 0;

	// index search variables
	node_pos *starting_node_pos = malloc(sizeof(node_pos));

	if(strcmp(condition_column_name, "rid") == 0)
		starting_node_pos->node = table->header_page->b_tree->root;
	else if (index != NULL) 
		starting_node_pos->node = index->b_tree->root;
	starting_node_pos->index = 0;

	printf("\nafter node_pos initializing\n");

	// sequential search variables
	int i = 0, j = 0; // i is page number, j is slot_array and k is for result set

	while(1) { 
		// check if the condition column is the rid or an index for quicker search, else perform a sequential search
		if(strcmp(condition_column_name, "rid") == 0) {
			printf("\npeforming record key search\n");
			recordKey = findRecordKeyFrom(table, starting_node_pos, atoi(condition_value));
			++starting_node_pos->index;				
		} else if(index != NULL) {
			printf("\npeforming index key search\n");
			IndexKey *indexKey = findIndexKeyFrom(index, starting_node_pos, condition_value);		
			if(indexKey != NULL) {
				recordKey = findRecordKey(table, indexKey->value);
				++starting_node_pos->index;				
			}
			free(indexKey);
		} else {
			record = sequentialSearch(condition_column_name, condition_value, table, i, j);
			if(record != NULL) {
				recordKey = findRecordKey(table, record->rid);
			
				// check which counters need incrementing (and decrementing)
				if(table->pages[recordKey->value->page_number]->records[table->pages[recordKey->value->page_number]->record_position - 1] == record) {
					i = recordKey->value->page_number + 1;	// move to the next page of the current records page
					j = 0;					// reset j back to 0 to start at the first record on the next page
				} else {
					i = recordKey->value->page_number;
					j = recordKey->value->slot_number + 1;	// else move to the next slot array position
				}	

				printf("\nafter sequential search %s, i = %d, k = %d, j = %d\n", record->data[0], i, k, j);				
			}
		}

		// if we have found a match for our query
		if(recordKey != NULL) {			
			record = table->pages[recordKey->value->page_number]->records[recordKey->value->slot_number];
		
			// get the target column data
			result[k] = malloc(record->size_of_data);
			
			getColumnData(record, target_column_name, result[k++], table->format);
			printf("\nresult[%d] %s\n", k - 1, result[k - 1]);

			free(recordKey);    
			recordKey = NULL; 
		} else {
			break;	
		}
	}

	free(starting_node_pos);

	// if we got at least one record, return the result set
	if(k > 0)
		return result;
	else
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
		printf("\ncfuhash_exists\n");
		// get table
		Table *table = (Table *) cfuhash_get(dataBuffer->tables, table_name);
		printf("\ndeleting table\n");	
		// free table (and all its entries)
		deleteTable(table);

		printf("cfuhashdelete");
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
                record = sequentialSearch(condition_column_name, condition_value, table, 0, 0);
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

	// locate field to be deleted
	int pos = locateField(table->format, column_name);			

	// shift deleted field + 1 down		
	int i, j, k, pc, rc;

	for(i = pos; i < table->format->number_of_fields -1; ++i) 
		table->format->fields[i] = table->format->fields[i + 1];
	table->format->fields[i] = NULL;

	// for each page of the table
	for(i = 0, pc = 0; pc < table->number_of_pages && i < MAX_TABLE_SIZE; ++i){
		if(table->pages[i] == NULL)
			continue;
	
		// for each record of that table
		for(j = 0, rc = 0; rc < table->pages[i]->number_of_records && j < MAX_RECORD_AMOUNT; ++j) {
			if(table->pages[i]->records[j] == NULL)
				continue;
			
			free(table->pages[i]->records[j]->data[pos]);		
	
			for(k = pos; k < table->format->number_of_fields -1; ++k) {
				printf("\nIN: %d - %s , %s\n", j, table->pages[i]->records[j]->data[k], table->pages[i]->records[j]->data[k + 1]);
				table->pages[i]->records[j]->data[k] = table->pages[i]->records[j]->data[k + 1];
			}

			table->pages[i]->records[j]->data[k] = NULL;
			table->pages[i]->records[j]->number_of_fields--;
						
			++rc;	
		}

		++pc;
	}
	
	
	table->format->number_of_fields--;

	return 0;
}

int alterTableChangeColumn(char *database_name, char *table_name, char *target_column, char *new_name) {
	Table *table = (Table *) cfuhash_get(dataBuffer->tables, table_name);

	int pos = locateField(table->format, target_column);
	
	strcpy(table->format->fields[pos]->name, new_name);
			
	return 0;
}

