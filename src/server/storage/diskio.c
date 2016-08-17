/*
 I NEED : data[BLOCK_SIZE] in each struct
	DO I NEED : struct records or could I just iterate through the slot array
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <errno.h>
#include "diskio.h"
#include "../../../libs/libbtree/btree.h"

#define MAX_TABLE_SIZE 5 // number of pages

int BLOCK_SIZE;
#define SLOT_SIZE 20 // how many records can the page hold TO DO should be a based on individual record size 

// HEADER PAGE LAYOUT
#define SIZE_BYTE 0
#define HIGHEST_RID_BYTE 1
#define INCREMENT_BYTE 5
#define NUMBER_OF_PAGES_BYTE 9
#define HEADER_PAGE_AVAILABLE_BYTE 13
#define BTREE_BYTE 17

// PAGE LAYOUT
#define PAGE_NUMBER_BYTE 0
#define PAGE_SPACE_AVAILABLE_BYTE 1
#define NUMBER_OF_RECORDS_BYTE 5
#define RECORD_TYPE_BYTE 9
#define SLOT_ARRAY_BYTE 13
#define RECORD_BYTE (SLOT_SIZE + SLOT_ARRAY_BYTE)

// RECORD LAYOUT
#define RID_BYTE 0
#define SIZE_OF_DATA_BYTE 3
#define SIZE_OF_RECORD_BYTE 7
#define DATA_BYTE 11

// RECORD PROPERTIES
#define VARIABLE_LENGTH 0
#define FIXED_LENGTH 1

// MAX values
#define MAX_RECORD_AMOUNT 2 
#define MAX_INDEX_SIZE 20000 // how big can the index file associated with a table be
#define MAX_INDEXES_AMOUNT 10

 
#define SEPARATOR " "

#define ORDER_OF_BTREE 5
#define MAX_NODE_AMOUNT MAX_RECORD_AMOUNT / ORDER_OF_BTREE   // maximum number of nodes per index. 

#define MAX_FORMAT_SIZE 30
#define MAX_FIELD_AMOUNT 20
#define MAX_FIELD_SIZE 50

// RECORD FUNCTIONALITY

Record * createRecord(char **data, int number_of_fields, int size_of_data){
        Record *record = malloc(sizeof(Record));
	record->rid = 0;
        record->size_of_data = size_of_data;
	record->number_of_fields = number_of_fields;
	record->data = data;
	// size of the whole record is the size of (some) of the members plus the data
	record->size_of_record = sizeof(record->rid) + sizeof(record->number_of_fields) + sizeof(record->size_of_data) + sizeof(record->size_of_record) + record->size_of_data;
	return record;
}

int insertRecord(Record *record, Page *page, Table *table) {
	record->rid = table->rid++;	

	// insert record into that page
	page->records[page->number_of_records] = record;	

	// add slot for new record inserted
	// the RValue returns an offset in bytes
	page->slot_array[page->number_of_records++] = BLOCK_SIZE - page->space_available - record->size_of_record;

	return 0;	
}

int commitRecord(Record *record, Table *table) {

        // get path to table file
        char path_to_table[50]; // TO DO
        int fd = open(path_to_table, O_RDWR);
	
	// get page of table where record is to be inserted
        int page_location = table->number_of_pages * BLOCK_SIZE;

        // mapped the file to memory starting at page_location
        char *map_page = mmap((caddr_t)0, BLOCK_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED, fd, page_location);

        // get the insert location of the new record
        int space_available = map_page[PAGE_SPACE_AVAILABLE_BYTE];
        int insert_location = BLOCK_SIZE - space_available;
	
	


	// TO DO check there is enough room for this last record

        // SERIALIZE record at position insert_location

        // get address of new record got from mmap append

        // insert this address into next available slot
	
	// update record count
	int number_of_records = map_page[NUMBER_OF_RECORDS_BYTE];
	++number_of_records;
	map_page[NUMBER_OF_RECORDS_BYTE] = number_of_records;        

        return 0;
}


// returns 0 on success and -1 otherwise
// NOTE: the slot_array[slot_number] still points to the record in memory in a rollback is requested
int deleteRow(Table *table, int page_number, int slot_number){
	
	printf("\n\t\t\t\tpage_number = %d, slot_number = %d\n", page_number, slot_number);
	if(table->header_page->b_tree->root == NULL)
		printf("\n\t\t\t\tShit is NULL\n");

	int rid = table->pages[page_number]->records[slot_number]->rid;

        // "delete" row.
	btree_delete_key(table->header_page->b_tree, table->header_page->b_tree->root, &rid);
	
	// TO DO
	// for each index of the table, delete their associated index nodes
	/*
	int i;
	for(i = 0; i < table->indexes->number_of_indexes; ++i){
		Index *index = table->indexes->indexes[i];
		btree_delete_key(index->b_tree, index->b_tree->root);
	}
	*/
	
	printf("\n\t\t\t\tAfter b-tree delete\n");
        table->pages[page_number]->records[slot_number] = NULL;	
	printf("\n\t\t\t\tAfter equals NULL\n");
	table->pages[page_number]->number_of_records--;
	printf("\n\t\t\t\tAfter --\n");
	if(table->pages[page_number]->number_of_records == 0) {
		table->pages[page_number] = NULL;
		table->number_of_pages--; 
		// TO DO
		// free page itself not the pointer
	}

	return 0;
}




// returns a single record where field equals value
Record * searchRecord(Table *table, char *field, char *value){
	
	Index *index = hasIndex(field, table);

	// check if the field is an index for quicker access
	if(index != NULL)
		return indexSearch(index, value, table); 	
	
	else // if not an index, perform sequential search for the record
		return sequentialSearch(field, value, table);
}


// returns index reference if field is an index in table, NULL otherwise
Index * hasIndex(char *field, Table *table) {
	int i;
	for(i = 0; i < table->indexes->number_of_indexes; ++i) {	
                if(strcmp(field, table->indexes->indexes[i]->index_name) == 0) 
			return table->indexes->indexes[i];
	}

	return NULL;
}


Record * indexSearch(Index *index, char *value, Table *table) {
	// get the btree key-value pair from the tree based on value
        bt_key_val * index_key_val = btree_search(index->b_tree, value);

	// if an entry for that value exists
        if(index_key_val != NULL) {
				
		// get the rid from that entry
		int rid = * (int *) index_key_val->val;

		// search the table b-tree based on the rid value
		bt_key_val * record_key_val = btree_search(table->header_page->b_tree, &rid);

		// if an entry exists
		if(record_key_val != NULL) {
			RecordKeyValue *recordKeyValue = (RecordKeyValue *) record_key_val->val;
			Page *page = table->pages[recordKeyValue->page_number];
			return page->records[recordKeyValue->slot_number]; 
		}		
	}

	return NULL;
}


Record * sequentialSearch(char *field, char *value, Table *table) {

	int i,j;

	// for each page of the table
	for(i = 0; i < table->number_of_pages; ++i){
		// for each record of that table
		for(j = 0; j < table->pages[i]->number_of_records; ++j) {
			if(hasValue(table, table->pages[i]->records[j], field, value) == 0){
					return table->pages[i]->records[j];
			}
		}
	}

	return NULL;
}

int hasValue(Table *table, Record * record, char *field, char *value) {
	Field **fields = table->format->fields;

        // search each field until target column is found
        int i;
        for(i = 0; i < table->format->number_of_fields; ++i) {
                if(strcmp(fields[i]->name, field) == 0){
			if(strcmp(record->data[i], value) == 0){			
				return 0;
			}
                }
        }

	return -1;		
}




// FIELD FUNCTIONALITY
int createField(char *type, char *name, Format *format) {
	Field *field = malloc(sizeof(Field));
	strcpy(field->type, type);
	strcpy(field->name, name);
	format->fields[format->number_of_fields++] = field;
	return 0;
}




// create a format struct from format "sql query"
int createFormat(Table *table, char *fields[], int number_of_fields) {
	Format *format = malloc(sizeof(Format));
	format->number_of_fields = 0;
			
	// for each field in format
	int i;
	int offset;
	char type[20];
	char name[MAX_FIELD_SIZE];
	for(i = 0; i < number_of_fields; ++i){	
		int type_length = setType(fields[i], type);
		setName(fields[i], type_length + 1, name); //+ 1 accounts for the space
		createField(type, name, format);
		format->format_size += strlen(fields[i]) * sizeof(fields[i][0]);
	}

	format->format_size += sizeof(format->fields);
	table->format = format;
	return 0;
}


int setType(char *field, char *destination) {
	int i;
	int type_length = 0;
	for(i = 0; field[i] != ' '; ++i){	
		destination[i] = field[i];
		++type_length;
	}
	destination[i] = '\0';
	return type_length; 
}


int setName(char *field, int position, char *destination) {
	int i, j;
	for(i = position, j = 0; field[i] != '\0'; ++i, ++j)
		destination[j] = field[i];
	destination[j] = '\0';
	return 0;
}


int getColumnData(Record *record, char *column_name, char *destination, Format *format) {
	Field **fields = format->fields;
	
	// search each field until target column is found
	int i;
	for(i = 0; i < format->number_of_fields; ++i) {
		if(strcmp(fields[i]->name, column_name) == 0){	
			// the ith segment of data from record->data
			strcpy(destination, record->data[i]);
			return 0;
		}	
	}

	// TO DO variable length processing
	/*
		If the record is variable length then each field will need to be defined per record
		For fixed length only the field lengths need to be defined once for all records
		This means the record layout and thus processing of a variable length record will be different
		This may require a byte before each field which defines the size of the immediate proceeding field
	*/

	return -1;
}



// PAGE FUNCTIONALITY
unsigned int value(void * key) {
        return *((int *) key);
}

unsigned int keysize(void * key) {
        return sizeof(int);
}

unsigned int datasize(void * data) {
        return sizeof(int);
}

HeaderPage* createHeaderPage(Table *table) {
	HeaderPage* header_page = malloc(sizeof(HeaderPage));
	header_page->space_available = BLOCK_SIZE;
	header_page->b_tree = btree_create(ORDER_OF_BTREE);
	header_page->b_tree->value = value;
	header_page->b_tree->key_size = sizeof(int);
        header_page->b_tree->data_size = sizeof(RecordKeyValue);
	table->header_page = header_page;
	table->size += header_page->space_available;
	return header_page;
}


Page* createPage(Table *table) {
        Page *page = malloc(sizeof(Page));
        page->number = table->number_of_pages;
        page->space_available = BLOCK_SIZE;
	page->number_of_records = 0;
	page->record_type = -1; // initialized to undefined
	table->pages[table->number_of_pages++] = page;
	table->size += BLOCK_SIZE;
        return page;
}




// RECORDKEY FUNCTIONALITY

RecordKey * createRecordKey(int rid, int page_number, int slot_number) {
	
	RecordKey * recordKey = malloc(sizeof(recordKey));
	recordKey->rid = rid;
	
	RecordKeyValue * recordKeyValue = malloc(sizeof(RecordKeyValue));
	recordKeyValue->page_number = page_number;
	recordKeyValue->slot_number = slot_number;
	
	recordKey->value = recordKeyValue;

	return recordKey;
}


int insertRecordKey(RecordKey *recordKey, Table *table){
	
	bt_key_val *key_val = malloc(sizeof(key_val));

	key_val->key = malloc(sizeof(recordKey->rid));
	*(int *)key_val->key = recordKey->rid;

	key_val->val = malloc(sizeof(RecordKeyValue));
	((RecordKeyValue *) (key_val->val))->page_number = recordKey->value->page_number;
	((RecordKeyValue *) (key_val->val))->slot_number = recordKey->value->slot_number;

	btree_insert_key(table->header_page->b_tree, key_val);

	return 0;
}


RecordKey * findRecordKey(Table *table, int key) {
	bt_key_val *key_val = btree_search(table->header_page->b_tree, &key);
	if(key_val != NULL) {
		RecordKey *recordKey = createRecordKey(*(int *)key_val->key, ((RecordKeyValue *) key_val->val)->page_number, ((RecordKeyValue *) key_val->val)->slot_number);
		return recordKey;
	} else {
		return NULL;
	}
}




// INDEX FUNCTIONALITY

// Indexes represent the index file
Indexes * createIndexes(Table *table) {
	Indexes *indexes = malloc(sizeof(Indexes));	
	indexes->space_available = MAX_INDEX_SIZE;
	indexes->number_of_indexes = 0;
	indexes->size = sizeof(indexes->space_available) + sizeof(indexes->size) + sizeof(indexes->number_of_indexes) + sizeof(indexes->indexes);
	table->indexes = indexes;	
	return indexes;
}


// TO DO
// pass the key size to the function. value size will be int
Index * createIndex(char *index_name, Indexes *indexes) {
	Index *index = malloc(sizeof(Index));
	index->b_tree = btree_create(ORDER_OF_BTREE);
	index->b_tree->value = value;
        index->b_tree->key_size = keysize;
        index->b_tree->data_size = datasize;
	strcpy(index->index_name, index_name);
	index->header_size = sizeof(index->index_name) + sizeof(index->header_size) + sizeof(index->btree_size);

	indexes->indexes[indexes->number_of_indexes++] = index;
	return index;
}


IndexKey * createIndexKey(char * key, int value) {
	
	IndexKey *indexKey = malloc(sizeof(IndexKey));

	indexKey->key = key;
	indexKey->value = value;
	indexKey->size_of_key = sizeof(indexKey->key) + sizeof(indexKey->value) + sizeof(indexKey->size_of_key);

	return indexKey;
}


int insertIndexKey(IndexKey *indexKey, Index *index) {
	bt_key_val * key_value = malloc(sizeof(key_value));
	key_value->key = malloc(strlen(indexKey->key) * sizeof(indexKey->key[0]));
	strcpy(key_value->key, indexKey->key);
	
	key_value->val = malloc(sizeof(int));
	* (int *)key_value->val = indexKey->value;
	btree_insert_key(index->b_tree, key_value);	
	return 0;
}

IndexKey * findIndexKey(Index *index, char *key){
	bt_key_val *key_val = btree_search(index->b_tree, key);
        if(key_val != NULL) {
                IndexKey *indexKey = createIndexKey((char *) key_val->key, *(int *) key_val->val);
                return indexKey;
        } else {
                return NULL;
        }
			
}


int createIndexFile(char *table_name) {
	// create file

	// begin writing to file
	
		// header information
		
			// size of the header

			// size of the file

			// number of indexes

			// index 1 name
				// index type
				// index length
				// location of index 1 (offset into the file)

			// index 2 name
				// index type
				// index length
				// location of index 2 (offset into the file )
			
			// ...
			
		
		// indexes

			// index 1 (its own block)
			
				// header

					// size of header

					// size of index

					// index name
				
					// number of nodes

				// B-TREE
	
				
}

int commitIndex(char *destination_file, Index *index) {
	// start from index, traverse through each indexes[] and in turn traverse through each indexNode[]
}






// TABLE FUNCTIONALITY
Table * createTable(char *table_name) {
	BLOCK_SIZE = getpagesize();

	Table *table = malloc(sizeof(Table));
	table->size = 0;
	table->rid = 0;
	table->increment = 10;		
	table->number_of_pages = 0; // we need to use number_of_pages as an index so we set it to 0
	createHeaderPage(table);
	createPage(table);
	
	// TO DO
	// create index based on primary key used to create table
	createIndexes(table);
	return table;
}


/*
	Open table and import data into structure
	Close the file and perform all operations on the struct Table rather then the original file
	When the user is finished editing the table, the commit it to memory using an almost identical procedure
*/
/*
Table *openTable(char *table_name, char *database) {
	
	char path_to_table[50];

	// concat database and table_name to get file path
	getPathToFile(".csd", table_name, database, path_to_table);

	// map entire table into memory for easy access
	char *map_table = mapTable(path_to_table);	
	
	// referebce mapped data into abstract structs 
	Table *table = initializeTable(map_table);
 
	// close mapped file
	munmap(map_table, BLOCK_SIZE);
		
	return table;
}


	char * mapTable(char * path_to_table) {
		int fd = open(path_to_table, O_RDWR);	
		char *map_table = mmap((caddr_t)0, BLOCK_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
		
		// find how many pages the table has
		int number_of_pages = map_table[NUMBER_OF_PAGES_BYTE];	

		// unmap the table and close file descriptor
		munmap(map_table, BLOCK_SIZE);
		close(fd);

		// map the table again fully with all pages
		map_table = mmap((caddr_t)0, (BLOCK_SIZE * number_of_pages), PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
	
		close(fd);
		
		return map_table;	
	}


	Table *initializeTable(char *map_table) {

		Table *table = malloc(sizeof(table));
		table->size = map_table[SIZE_BYTE];
		table->rid = map_table[HIGHEST_RID_BYTE];
		table->increment = map_table[INCREMENT_BYTE];
		table->number_of_pages = map_table[NUMBER_OF_PAGES_BYTE]; 
		table->header_page->space_available = map_table[HEADER_PAGE_AVAILABLE_BYTE];
		// TO DO reference B-Tree


		// TO DO Map format structs

		// TO DO Map field structs

		// map each of the pages		
		
		// for each page		
		int i;
		for(i = 0; i < table->number_of_pages; ++i) {
			int page_offset = BLOCK_SIZE * i;
			Page *page = malloc(sizeof(page));
			page->number = map_table[PAGE_NUMBER_BYTE + page_offset];
			page->space_available = map_table[PAGE_SPACE_AVAILABLE_BYTE + page_offset];
			page->number_of_records = map_table[NUMBER_OF_RECORDS_BYTE + page_offset];
			page->record_type = map_table[RECORD_TYPE_BYTE + page_offset];
			

			// for each slot of this page
			int j;
			int slot_offset;
			for(j = 0, slot_offset = 0; j < SLOT_SIZE; j++, slot_offset += sizeof(int)) // the byte needs to be offset by the sizeof the int 
				page->slot_array[j] = map_table[(SLOT_ARRAY_BYTE + page_offset) + slot_offset];

			// for each record of this page
			
			// use the record length as an offset (in bytes)
			int record_length = 0;
			for(j = 0; j < page->number_of_records; ++j) {
				page->records[j] = malloc(sizeof(page->records[j]));
				page->records[j]->rid = map_table[(RECORD_BYTE + RID_BYTE) + record_length];
				page->records[j]->number_of_fields = map_table[(RECORD_BYTE + NUMBER_OF_FIELDS_BYTE) + record_length]
				page->records[j]->size_of_data = map_table[(RECORD_BYTE + SIZE_OF_DATA_BYTE) + record_length];
				page->records[j]->size_of_record = map_table[(RECORD_BYTE + SIZE_OF_RECORD_BYTE) + record_length];
				
				// TO DO
				// for each column of data
				int k;
				for(k = 0; k < page->records[j]->number_of_fields; ++k) {
					// TO DO
					// get field type
					// find out which type it is (switch statement)
					// allocate memory of that size for data[k]
					page->records[j]->data[k] = malloc(sizeof(table->format->fields[k]));
					mapData(RECORD_BYTE + DATA_BYTE + record_length, map_table, page->records[j]->data[k]);  // map_table[(RECORD_BYTE + DATA_BYTE) + record_length];
}
				record_length += page->records[j]->size_of_record;
				// assign each member of page.records manually
			}		
	
			table->pages[i] = page;			
		}
			
		return table;
	}
		
		int mapData(int start_location, char *map, char *destination, Format *format) {
			int i, j;
			for(i = start_location, j = 0; map[i] != '\n'; ++i, ++j){
				destination[j] = map[i];
			
			}
			destination[i] = '\0';
			return 0;
		}
*/


// TO DO
int closeTable(Table *table) {
	// traverse through the entire table and free up the memory
}



// FILE I/O
int commitTable(char *table_name, Table *table, char *database_name) {
	
	// if table file exists
		// get path to each file associated with the table
		char path_to_table[50];
		getPathToFile(".csd", table_name, database_name, path_to_table);

		char path_to_index[50];
		getPathToFile(".csi", table_name, database_name, path_to_index);

		char path_to_format[50];
		getPathToFile(".csf", table_name, database_name, path_to_format); 

	// else 
		// create database folder
		createFolder(database_name);

		// create .csi (index file), .csd (table file) and .csf(format file)


	// insert data from structs to table files

		
	// NOTE: commiting each record involves looping through each page->records[i] up to number_of_records and commiting only those that are not NULL, skip those that are

	
	
	// [TABLE_SIZE - CURRENT_MAX_RID - INCREMENT_AMOUNT - PAGE_NUMBER - SPACE_AVAILABLE]
	char data[BLOCK_SIZE];
	data[SIZE_BYTE] = table->size;
	data[RID_BYTE] = table->rid;
	data[INCREMENT_BYTE] = table->increment;
	data[HEADER_PAGE_AVAILABLE_BYTE] =  table->header_page->space_available;
		
	// TO DO: Commit each page of the table
	// for each page of the table (use multiple of BLOCK_SIZE i.e. 2nd page starts at BLOCK_SIZE, 3rd page starts at BLOCK_SIZE x 2)

	return 0;	
}


// LOCATING FILE
int getPathToFile(char *file_extension, char *table_name, char *database, char *destination) {

	int i;
	for(i = 0; i < strlen(database); ++i) {
		destination[i] = database[i];
	}

	destination[i++] = '/';

	int j;
	for(j = 0; j < strlen(table_name); ++i, ++j) {
		destination[i] = table_name[j];
	}

	for(j = 0; j < strlen(file_extension); ++j, ++i)
		destination[i] = file_extension[j];

	destination[i] = '\0';
}



// INDEX FILE


// DATABASE
int createFolder(char *folder_name) {
	struct stat st = {0};
	char full_path[50] = "data/";
	strcat(full_path, folder_name);

	if (stat(full_path, &st) == -1) {
		errno = 0;
    		if(mkdir(full_path, 0777) == -1)
			printf("\n\t\terrno = %s\n", strerror(errno));		
		return 0;
	}
}


int deleteFolder(char *folder_name) {
	// delete all files within database folder
	char full_command[100] = "exec rm -r ";
	char path[50] = "data/";
	strcat(path, folder_name);
	strcat(full_command, path);
	strcat(full_command, "/*");
	system(full_command);

	// delete folder itself
	return rmdir(path);	
}
