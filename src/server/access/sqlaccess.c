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


int create(char *table_name, char *column_names[], char *data_types[], int number_of_fields) {
	Table *table = createTable(table_name);
	createIndexes(table);
	createFormat(table, column_names, data_types, number_of_fields);
	printf("\nformat: %d, table_name %s\n", table->format->number_of_fields, table_name);
	addTableToBuffer(table_name, table);		
	return 0;
}


int addConstraintPrimaryKeys(char *target_table_name, int number_of_primary_keys, char **primary_keys) {

	Table *target_table = (Table *) cfuhash_get(dataBuffer->tables, target_table_name);

	int i;
	for(i = 0; i < number_of_primary_keys; ++i) {	
		int pos = locateField(target_table->format, primary_keys[i]);		
		addPrimaryKey(target_table, target_table->format->fields[pos]);	
	}

}


int addConstraintForeignKeys(char *target_table_name, int number_of_foreign_keys, char **foreign_keys, char **foreign_key_names, char **foreign_key_tables) {

	Table *target_table = (Table *) cfuhash_get(dataBuffer->tables, target_table_name);

	int i;
	for(i = 0; i < number_of_foreign_keys; ++i) {	
		Table *origin_table = (Table *) cfuhash_get(dataBuffer->tables, foreign_key_tables[i]);
		int pos_origin = locateField(origin_table->format, foreign_key_names[i]);		
		int pos = locateField(target_table->format, foreign_keys[i]);
		addForeignKey(target_table, origin_table, origin_table->format->fields[pos_origin], target_table->format->fields[pos]);	
	}

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
	Record *record = createRecord(data, getNumberOfFields(getTableFormat(table)), size);

	// get last page to insert record
	Page *page = getPage(table, getNumberOfPages(table) - 1);
	
	// check if there is enough room for the new record
	// is there enough room on the page to insert record
        if((getPageSpaceAvailable(page) - getRecordSize(record)) <= 0) {
		printf("\nCREATING NEW PAGE\n");
		page = createPage(table); // create a new page
	}
               
	// insert record into table 
	insertRecord(record, page, table);

	// create a table record key from the record to place in a B-Tree node
	RecordKey *recordKey = createRecordKey(getRecordRid(record), getPageNumber(page), getPageNumberOfRecords(page) - 1);

	// insert node into table B-Tree
	insertRecordKey(recordKey, table);

	// INDEXES
	// get set of indexes associated with table
	Indexes *indexes = getIndexes(table);

	// for every index of the table (excluding primary index)
	int i;
	char buffer[100];
	for(i = 0; i < getNumberOfIndexes(indexes); ++i) {
		
		// fetch the data located underneath that column
		getColumnData(record, getIndexName(getIndexNumber(indexes, i)), buffer, getTableFormat(table));			
		// create index key (from newly inserted record)
		IndexKey *indexKey = createIndexKey(buffer, getRecordRid(record));	
		// insert index key into index
		insertIndexKey(indexKey, getIndexNumber(indexes, i));	
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
		deleteRow(table, getRecordKeyPageNumber(recordKey), getRecordKeySlotNumber(recordKey));
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


char *getRecordData(Table *table, char *target_column_name, int page_number, int slot_number) {
	Record *record = getRecord(table, page_number, slot_number);		
	char *result = malloc(record->size_of_data);
	
	getColumnData(record, target_column_name, result, getTableFormat(table));

	return result;
}


char **selectRecordRid(Table *table, char *target_column_name, char *condition_column_name, char *condition_value) {

	printf("\npeforming record key search\n");

	Record *record;
       	RecordKey *recordKey = NULL;

	// result set variables
	char **result = malloc(sizeof(char *));
	
	recordKey = findRecordKey(table, atoi(condition_value));				
	
	// if we have found a match for our query
	if(recordKey != NULL) {			
		result[0] = getRecordData(table, target_column_name, getRecordKeyPageNumber(recordKey), getRecordKeySlotNumber(recordKey));	
		free(recordKey);    
		recordKey = NULL; 
		return result;
	} else {
		free(result);
		return NULL;
	}
}


char **selectRecordIndex(Table *table, Index *index, char *target_column_name, char *condition_column_name, char *condition_value) {

	Record *record;       
        RecordKey *recordKey = NULL;

	// result set variables
	char **result = malloc(getHighestRid(table) * sizeof(char *));
	int k = 0;

	// index search variables
	node_pos *starting_node_pos = malloc(sizeof(node_pos));
	starting_node_pos->node = getIndexBtreeRoot(index);
	starting_node_pos->index = 0;

	while(1) { 
		IndexKey *indexKey = findIndexKeyFrom(index, starting_node_pos, condition_value);		
		if(indexKey != NULL) {
			recordKey = findRecordKey(table, getIndexKeyValue(indexKey));
			++starting_node_pos->index;				
			free(indexKey);
		}
		
		// if we have found a match for our query
		if(recordKey != NULL) {			
			result[k++] = getRecordData(table, target_column_name, getRecordKeyPageNumber(recordKey), getRecordKeySlotNumber(recordKey));	
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
	else {
		free(result);
		return NULL;
	}

}


char **selectRecordSequential(Table *table, char *target_column_name, char *condition_column_name, char *condition_value) {

       	Record *record = NULL;
        RecordKey *recordKey = NULL;

	// result set variables
	// mutiple records may be found
	char **result = malloc(table->rid * sizeof(char *));
	int k = 0;

	printf("\nafter node_pos initializing\n");

	// sequential search variables
	int i = 0, j = 0; // i is page number, j is slot_array and k is for result set

	while(1) { 
		record = sequentialSearch(condition_column_name, condition_value, table, i, j);
		
		printf("\nstart of sequential search\n");
	
		if(record != NULL) {
			recordKey = findRecordKey(table, getRecordRid(record));			
			Page *page = getPage(table, getRecordKeyPageNumber(recordKey));

			// check which counters need incrementing (and decrementing)
			if(getLastRecordOfPage(page) == record) {
				i = getRecordKeyPageNumber(recordKey) + 1;	// move to the next page of the current records page
				j = 0;					// reset j back to 0 to start at the first record on the next page
			} else {
				i = getRecordKeyPageNumber(recordKey);
				j = getRecordKeySlotNumber(recordKey) + 1;	// else move to the next slot array position
			}	

			printf("\nafter sequential search %s, i = %d, k = %d, j = %d\n", record->data[0], i, k, j);				
		}

		// if we have found a match for our query
		if(recordKey != NULL) {				
			result[k++] = getRecordData(table, target_column_name, getRecordKeyPageNumber(recordKey), getRecordKeySlotNumber(recordKey));	
			free(recordKey);    
			recordKey = NULL;
		} else {
			break;	
		}
	}	

	// if we got at least one record, return the result set
	if(k > 0)
		return result;
	else {
		free(result);		
		return NULL;
	}
}


// returns target column data
char **selectRecord(char *database_name, char *table_name, char *target_column_name, char *condition_column_name, char *condition_value) {
	
	Table *table = (Table *) cfuhash_get(dataBuffer->tables, table_name);
	Index *index = NULL;    
	
	// check if the condition column is the rid or an index for quicker search, else perform a sequential search
	if(strcmp(condition_column_name, "rid") == 0)
		return selectRecordRid(table, target_column_name, condition_column_name, condition_value);	
	else if((index = hasIndex(condition_column_name, table)) != NULL) 
		return selectRecordIndex(table, index, target_column_name, condition_column_name, condition_value); 
	else 
		return selectRecordSequential(table, target_column_name, condition_column_name, condition_value);		
	
}


int commit(char *table_name, char *database_name) {
	Table *table = cfuhash_get(dataBuffer->tables, table_name);

	commitTable(table, table_name, database_name);
	
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
                        recordKey = findRecordKey(table, getIndexKeyValue(indexKey));
                free(indexKey);
        } else {
                record = sequentialSearch(condition_column_name, condition_value, table, 0, 0);
                if(record != NULL)
                        recordKey = findRecordKey(table, getRecordRid(record));
        }

        // if we have found a match for our delete query, then delete that row from the table
        if(recordKey != NULL) {
                record = getRecord(table, getRecordKeyPageNumber(recordKey), getRecordKeySlotNumber(recordKey));
		int pos = locateField(getTableFormat(table), target_column_name);
        	setDataAt(record, pos, target_column_value);
                free(recordKey);
	}

	return -1;	
}


int alterTableAddColumns(char *table_name, char **identifiers, char **types, int number_of_identifiers) {
	// get table
	Table *table = (Table *) cfuhash_get(dataBuffer->tables, table_name);

	int i;
	for(i = 0; i < number_of_identifiers; ++i) {
		// create new field setting field and type
		createField(types[i], identifiers[i], table->format);
	}
}


int alterTableDropColumns(char *table_name, char **column_names, int number_of_columns) {

	// get table
	Table *table = (Table *) cfuhash_get(dataBuffer->tables, table_name);

	// locate field to be deleted
	Format *format = getTableFormat(table);

	int x;
	for(x = 0; x < number_of_columns; ++x) {

		int pos = locateField(format, column_names[x]);			

		// shift deleted field + 1 down		
		int i, j, k, pc, rc;

		Field *field = NULL;
		// IDEA:
		// could be getNumberOfFields - pos
		for( i = pos, field = getField(format, i); i < getNumberOfFields(format) -1; ++i, field = getField(format, i)) 
			table->format->fields[i] = table->format->fields[i + 1];
		table->format->fields[i] = NULL;	// set the previously unavailable field to available

		Page *page = NULL;
		Record *record = NULL;
		char *data = NULL;
		// for each page of the table
		for(i = 0, pc = 0; pc < getNumberOfPages(table) && i < MAX_TABLE_SIZE; ++i){
			page = getPage(table, i);

			if(page == NULL)
				continue;
		
			// for each record of that table
			for(j = 0, rc = 0; rc < getPageNumberOfRecords(page) && j < MAX_RECORD_AMOUNT; ++j) {
				record = getRecord(table, i, j);

				if(record == NULL)
					continue;
				
				free(getDataAt(record, pos));	
				
				for(k = pos; k < getNumberOfFields(format) -1; ++k) {
					printf("\nIN: %d - %s , %s\n", j, table->pages[i]->records[j]->data[k], table->pages[i]->records[j]->data[k + 1]);
					table->pages[i]->records[j]->data[k] = table->pages[i]->records[j]->data[k + 1];
				}

				table->pages[i]->records[j]->data[k] = NULL;
							
				++rc;	
			}

			++pc;
		}
		
		
		table->format->number_of_fields--;

	}

	return 0;
}


int alterTableRenameTable(char *table_name, char *new_name) {
	// cfuhash_replace	
	//Table *table = (Table *) cfuhash_get(dataBuffer->tables, table_name);
	
}


int alterTableRenameColumn(char *table_name, char *target_column, char *new_name) {
	Table *table = (Table *) cfuhash_get(dataBuffer->tables, table_name);

	Format *format = getTableFormat(table);
	int pos = locateField(format, target_column);
	Field *field = getField(format, pos);	
	setName(field, new_name);
			
	return 0;
}

