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

#define ORDER_OF_BTREE 2
#define MAX_NODE_AMOUNT MAX_RECORD_AMOUNT / ORDER_OF_BTREE   // maximum number of nodes per index. 

#define MAX_FORMAT_SIZE 30
#define MAX_FIELD_AMOUNT 20
#define MAX_FIELD_SIZE 50

const char STARTING_NODE_MARKER = '{';
const char ENDING_NODE_MARKER = '}';


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



// returns 0 on success and -1 otherwise
// NOTE: the slot_array[slot_number] still points to the record in memory in a rollback is requested
int deleteRow(Table *table, int page_number, int slot_number){
	
	// get the record to be deleted
	Record *record = table->pages[page_number]->records[slot_number];

        // delete the records associated b-tree entry in the table b-tree
	btree_delete_key(table->header_page->b_tree, table->header_page->b_tree->root, &record->rid);
	
	// delete the records associated b-tree entries for each index of that table
	// for each index of the table, delete their associated index nodes
	int i;
	char buffer[50];
	Index *index;
	for(i = 0; i < table->indexes->number_of_indexes; ++i){
		index = table->indexes->indexes[i];
		getColumnData(record, index->index_name, buffer, table->format);
		btree_delete_key(index->b_tree, index->b_tree->root, buffer);
	}
	
	// set the pages record slot array to NULL
        table->pages[page_number]->records[slot_number] = NULL;	

	// decrement the pages record count
	table->pages[page_number]->number_of_records--;
	
	// check if no records left on the page
	if(table->pages[page_number]->number_of_records == 0) {
		// if no more records left, delete the page
		table->pages[page_number] = NULL;
		table->number_of_pages--; 
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


/*
	Some intermediate page entries may be NULL as a result of that pages records being deleted
	Therefore we need two counters, one that increments for each page slot and another that only increments when a nonon-NULL page is encountered
*/
Record * sequentialSearch(char *field, char *value, Table *table) {

	int pc, rc, i, j;

	printf("\n\t\t\t\tnumber_of_pages = %d\n", table->number_of_pages);
	
	// for each page of the table
	for(i = 0, pc = 0; pc < table->number_of_pages && i < MAX_TABLE_SIZE; ++i){
		printf("\n\t\t\t\ttable->pages[%d]\n", i);	
		if(table->pages[i] == NULL)
			continue;
		printf("\n\t\t\t\ttable->pages[%d] NOT NULL\n", i);	
		// for each record of that table
		for(j = 0, rc = 0; rc < table->pages[i]->number_of_records && j < MAX_RECORD_AMOUNT; ++j) {
			printf("\n\t\t\t\trecords[%d]\n", j);
			if(table->pages[i]->records[j] == NULL)
				continue;
			printf("\n\t\t\t\tRecords not NULL\n");
			if(hasValue(table, table->pages[i]->records[j], field, value) == 0)
				return table->pages[i]->records[j];
			
			++rc;	
		}

		++pc;
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





// FORMAT
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



// BTREE FUNCTIONALITY

unsigned int value(void * key) {
        return *((int *) key);
}


unsigned int keysize(void * key) {
        return sizeof(int);
}


unsigned int datasize(void * data) {
        return sizeof(int);
}

int getSizeOf(char *type) {
	if(strcmp(type, "VARCHAR") == 0)
		return 255;

	if(strcmp(type, "CHAR") == 0)
                return sizeof(char);	

	if(strncmp(type, "CHAR(", strlen("CHAR(")) == 0) {
		char c = type[strlen("CHAR(")];
		printf("\n%c\n", c);
		return c - '0';
	}

	if(strcmp(type, "INT") == 0)
                return sizeof(int);

	if(strcmp(type, "DOUBLE") == 0)
                return sizeof(double);

	return -1;
}


btree * createBtree(char *key_type, char *value_type, int key_size, int data_size) {
	btree *btree = btree_create(ORDER_OF_BTREE);
        btree->value = value;
        btree->key_size = key_size;
        btree->data_size = data_size;
	strcpy(btree->key_type, key_type);
	strcpy(btree->value_type, value_type);
	btree->number_of_entries = 0;	
	return btree;
}





// PAGE FUNCTIONALITY

HeaderPage* createHeaderPage(Table *table) {
	HeaderPage* header_page = malloc(sizeof(HeaderPage));
	header_page->b_tree = createBtree("INT", "RECORD", sizeof(int), sizeof(RecordKeyValue));
       	header_page->space_available = BLOCK_SIZE - sizeof(header_page->space_available); // everytime we add a new b-tree node we increase the size by that node
	table->header_page = header_page;
	return header_page;
}


HeaderPage * openHeaderPage(Table *table, FILE *fp) {

	HeaderPage *header_page = malloc(sizeof(HeaderPage));

	fread(&header_page->space_available, sizeof(header_page->space_available), 1, fp);

        // TO DO, fwrite ORDER_OF_BTREE before writing tree
        fwrite(ORDER_OF_BTREE, sizeof(ORDER_OF_BTREE), 1, fp);

        // TO DO, fwrite number_of_nodes before
        fread(&header_page->b_tree->number_of_entries, sizeof(header_page->b_tree->number_of_entries), 1, fp);

        // TO DO, fwrite key type and value type
        fread(&header_page->b_tree->key_type, sizeof(header_page->b_tree->key_type), 1, fp);
        fread(&header_page->b_tree->value_type, sizeof(header_page->b_tree->value_type), 1, fp);

	btree *btree = createBtree("INT", "RECORD", sizeof(int), sizeof(RecordKeyValue));

	// create bt_node
	btree->root = deserializeTree(fp, "TABLE", (Table *)table);	
}


Page* createPage(Table *table) {
        Page *page = malloc(sizeof(Page));
        page->number = table->number_of_pages;
        page->space_available = BLOCK_SIZE;
	page->number_of_records = 0;
	page->record_type = -1; // initialized to undefined
	table->pages[table->number_of_pages++] = page;
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
	table->header_page->b_tree->number_of_entries++;

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
Index * createIndex(char *index_name, Table *table) {
	Index *index = malloc(sizeof(Index));
	
	strcpy(index->index_name, index_name);

	// get index name (key) type
	int i;
	char key_type[50];
	for(i = 0; i < table->format->number_of_fields; ++i) {
		if(strcmp(table->format->fields[i]->name, index_name) == 0) {
			strcpy(key_type, table->format->fields[i]->type);
			break;
		}			
	}		

	int key_size = getSizeOf(key_type);	
	printf("\n\t\t\tkey_size = %d\n", key_size);
	
	index->b_tree = createBtree(key_type, "INT", key_size, sizeof(int));
	
	index->header_size = sizeof(index->index_name) + sizeof(index->header_size) + sizeof(index->btree_size);
	table->indexes->indexes[table->indexes->number_of_indexes++] = index;

	return index;
}


Index * getIndex(char *index_name, Table *table) {
	int i;
	for(i = 0; i < table->indexes->number_of_indexes; ++i) {
		if(strcmp(table->indexes->indexes[i]->index_name, index_name) == 0)
			return table->indexes->indexes[i];
	}

	return NULL;
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

	if(strcmp(index->b_tree->key_type, "VARCHAR") == 0) {
		printf("\n\t\tkey_length = %d\n", strlen(indexKey->key));
		key_value->key_length = strlen(indexKey->key);
	} else {
		key_value->key_length = 0;
	}

	index->b_tree->number_of_entries++;
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






// TABLE FUNCTIONALITY
Table * createTable(char *table_name) {
	BLOCK_SIZE = getpagesize();

	Table *table = malloc(sizeof(Table));
	table->rid = 0;
	table->increment = 10;		
	table->number_of_pages = 0; // we need to use number_of_pages as an index so we set it to 0
	table->size = sizeof(table->size) + sizeof(table->rid) + sizeof(table->increment) + sizeof(table->number_of_pages);
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


// traverse through the entire table and free up the memory
int deleteTable(Table *table) {
	int pc, i;
	
	freeHeaderPage(table->header_page);
	
	// for each page of the table
	for(i = 0, pc = 0; pc < table->number_of_pages && i < MAX_TABLE_SIZE; ++i){
		if(table->pages[i] != NULL) {
			freePage(table->pages[i]);
			++pc;
		}
	}

	freeFormat(table->format);

	freeIndexes(table->indexes);

	free(table);

	// TO DO
	// insert into an array the map_page position of this deleted table so a callback request can be completed

	// TO DO
        // notify {x} when committing to remove the table from memory
        // make committing actually removes the table from disk
}


int freeHeaderPage(HeaderPage *headerPage) {
	// destroy btree
	btree_destroy(headerPage->b_tree);

	// free page
	free(headerPage);

	return 0;
}


int freeFormat(Format *format){
	int i;
	for(i = 0; i < format->number_of_fields; ++i)
		free(format->fields[i]);

	free(format);

	return 0;
}


int freeIndexes(Indexes *indexes){
	int i;	
	for(i = 0; i < indexes->number_of_indexes; ++i)
		freeIndex(indexes->indexes[i]);

	free(indexes);	

	indexes = NULL;

	return 0;
}


int freeIndex(Index *index) {
	btree_destroy(index->b_tree);
}


int freePage(Page *page) {
	int rc, i;	
	for(i = 0; rc < page->number_of_records && i < MAX_RECORD_AMOUNT; ++i) {
		if(page->records[i] != NULL) {
			freeRecord(page->records[i]);
			++rc;
		}
	}

	free(page);

	return 0;
}


int freeRecord(Record *record) {
	int i;
	for(i = 0; i < record->number_of_fields; ++i)
		free(record->data[i]);

	free(record->data);

	free(record);

	return 0;
}






// COMMITS

/*
        The tree itself is represented by flattening it in prefix order. Each node is defined either to have children or not to have children. If a node is defined not to have children, the next physically succeeding node is a sibling. If a node is defined to have children, the next physically succeeding node is its first child. Additional children are represented as siblings of the first child. A chain of sibling entries is terminated by a null node.
*/

int serializeHeader(bt_node *node, FILE *fp) {

	// number of key-value pairs
	fwrite(&node->nr_active, sizeof(node->nr_active), 1, fp);
	printf("\n\t\t\t\tnr_active = %d\n", node->nr_active);

	// leaf node or not
	fwrite(&node->leaf, sizeof(node->leaf), 1, fp);
	printf("\n\t\t\t\tleaf %d\n", node->leaf);

	// number of children
	printf("\n\t\t\t\tlevel = %d\n", node->level);
	fwrite(&node->level, sizeof(node->level), 1 , fp);

	return 0;
}


int serializeTree(btree *btree, bt_node * node, FILE *fp){

	printf("\n\t\t\t\tIn serialize\n");

	// signal starting new node
	fwrite(&STARTING_NODE_MARKER, sizeof(STARTING_NODE_MARKER), 1, fp);

	serializeHeader(node, fp);

        // serialize each key value pair of this node
        int i;
        for(i = 0; i < node->nr_active; ++i) {
               	
                if(strcmp(btree->value_type, "RECORD") == 0) {
			printf("\n\t\t\t\tInside = RECORD\n", btree->value_type);
			printf("\n\t\t\tnode->key_vals[i]->key = %d, node->key_vals[i]->val->page_number = %d, node->key_vals[i]     ->val->slot_number = %d\n", *(int *) node->key_vals[i]->key, ((RecordKeyValue *) (node->key_vals[i]->val))->page_number,      ((RecordKeyValue *) (node->key_vals[i]->val))->slot_number);
                        fwrite((int *) node->key_vals[i]->key, sizeof(node->key_vals[i]->key), 1, fp);
		        fwrite(&((RecordKeyValue *) (node->key_vals[i]->val))->page_number, sizeof(((RecordKeyValue *) (node->key_vals[i]->val))->page_number), 1, fp);
			fwrite(&((RecordKeyValue *) (node->key_vals[i]->val))->slot_number, sizeof(((RecordKeyValue *) (node->key_vals[i]->val))->slot_number), 1, fp);
                } else {
                 	printf("\n\t\t\tnode->key_vals[i]->key = %s, node->key_vals[i]->val = %i\n", node->key_vals[i]->key,  *(int *) node->key_vals[i]->val);
        		printf("\n\t\t\ttype = %s\n", btree->key_type);	
			if(strncmp(btree->key_type, "CHAR(", strlen("CHAR(")) == 0) {
				printf("\n\tIm here %d\n", (unsigned int *) btree->key_size);
				fwrite(node->key_vals[i]->key, (unsigned int) btree->key_size * sizeof(char), 1, fp);
			 } else if (strcmp(btree->key_type, "VARCHAR") == 0) {
				// fwrite that value in
				printf("\n\t\t\tnode->key_vals[i]->key_length = %d, size = %d\n", node->key_vals[i]->key_length, sizeof(node->key_vals[i]->key_length));
				fwrite(&node->key_vals[i]->key_length, sizeof(node->key_vals[i]->key_length), 1, fp);				
				// fwrite the the data up to size			
				fwrite(node->key_vals[i]->key, node->key_vals[i]->key_length * sizeof(char), 1, fp);
			}
	
                        fwrite((int *)node->key_vals[i]->val, sizeof(node->key_vals[i]->val), 1, fp);			
                }
        }

	// signal end node
	fwrite(&ENDING_NODE_MARKER, sizeof(ENDING_NODE_MARKER), 1, fp);

        // if the node has children
        if(node->leaf == false){
		printf("\n\t\t\t\tNode is not leaf\n");
                // for each child node of the current node
                for(i = 0 ; i < node->nr_active + 1; i++)
                        serializeTree(btree, node->children[i], fp);
	}
}





bt_node * deserializeTree(FILE *fp, char *tree_type, Table *table) {

	bt_node *node = (bt_node *)mem_alloc(sizeof(bt_node));
	printf("\n\t\t\tIn deserialize\n");
	node->next = NULL;

	char start = 0;
	fread(&start, sizeof(start), 1, fp);
	printf("\n\t\t\t\tstart character %c\n", start);

	fread(&node->nr_active, sizeof(node->nr_active), 1, fp);
	node->key_vals = (bt_key_val **)mem_alloc(2*ORDER_OF_BTREE*sizeof(bt_key_val*) - 1);
	printf("\n\t\t\t\tnumber_of_keys %d\n", node->nr_active);

	fread(&node->leaf, sizeof(node->leaf), 1, fp);
	printf("\n\t\t\t\tnode->leaf %d\n", node->leaf);

	fread(&node->level, sizeof(node->level), 1, fp);        	
	printf("\n\t\t\t\tlevel %u\n", node->level);

	if(strcmp(tree_type, "TABLE") == 0)
		deserializeTableTree(table, fp, node, node->nr_active);	
	else // its an index tree
		deserializeIndexTree(table, tree_type, fp, node, node->nr_active);

	char end = 0;
	fread(&end, sizeof(end), 1, fp);
	printf("\n\t\t\t\tend character %c\n", end);

	if(node->leaf == false) {
		printf("\n\t\t\t\tleaf is not false\n");
		node->children = (bt_node **)mem_alloc(2*ORDER_OF_BTREE*sizeof(bt_node*));   		
		int i;
		for(i = 0; i < node->nr_active + 1; ++i)
			node->children[i] = deserializeTree(fp, tree_type, table);  
	}

	return node;			
}


int deserializeIndexTree(Table *table, char *index_name, FILE *fp, bt_node *node, int number_of_entries){
	
	printf("\n\t\t\t\tInside Index Tree\n");
	// get index
	Index *index = getIndex(index_name, table);
	printf("\n\t\t\t\tAfter Get Index\n");

	int i;	
	for(i = 0; i < number_of_entries; ++i) {		
		bt_key_val *key_val = malloc(sizeof(key_val));
		printf("\n\t\t\t\tAfter malloc\n");


		if (key_val == NULL) {
        		printf("Out of memory\n");
        		exit(-1);
    		}
	
		if(strcmp(index->b_tree->key_type, "VARCHAR") == 0 )	{
			printf("\n\t\t\t\tIn VARCHAR\n");
			key_val->key_length = 0;
			printf("\n\t\t\t\tkey_length = %i, size = %d\n", key_val->key_length, sizeof(key_val->key_length));
			//fread(&(key_val->key_length), sizeof(key_val->key_length), 1 , fp);
			unsigned int demo = 0;
			fread(&demo, sizeof(demo), 1 , fp);
			printf("\n\t\t\tAfter fread = %d\n", demo);
			key_val->key_length = demo;
			key_val->key = malloc(key_val->key_length * sizeof(char));
			fread(key_val->key, key_val->key_length * sizeof(char), 1, fp);
		} else {
			key_val->key = malloc((unsigned int) index->b_tree->key_size * sizeof(char));
			fread(key_val->key, (unsigned int) index->b_tree->key_size * sizeof(char), 1, fp);
		}
		
       		printf("\n\t\t\t\tnode->key_vals->key = %s\n", key_val->key);

        	key_val->val = malloc(sizeof(int));
		fread(key_val->val, sizeof(key_val->val), 1, fp);
         	printf("\n\t\t\t\tnode->key_vals->val = %i\n", *(int *) key_val->val);

        	node->key_vals[i] = key_val;
        	index->b_tree->number_of_entries++;
		printf("\n\t\t\t\tAfter number_of_entries\n");
	}
	
	return 0;
}


int deserializeTableTree(Table *table, FILE *fp, bt_node *node, int number_of_entries) {

	printf("\n\t\t\t\tin deserialize table tree\n");

	int i;
	for(i = 0; i < number_of_entries; ++i) {

		bt_key_val *key_val = malloc(sizeof(key_val));

        	key_val->key = malloc(sizeof(int));
 		fread(key_val->key, sizeof(key_val->key), 1, fp); 
		printf("\n\t\t\t\tnode->key_vals->key = %i\n", *(int *) key_val->key);

        	key_val->val = malloc(sizeof(RecordKeyValue));
		fread(&((RecordKeyValue *) (key_val->val))->page_number, sizeof(((RecordKeyValue *) (key_val->val))->page_number), 1, fp);
		fread(&((RecordKeyValue *) (key_val->val))->slot_number, sizeof(((RecordKeyValue *) (key_val->val))->page_number), 1, fp);
         	printf("\n\t\t\t\tnode->key_vals->val->page_number = %i\n",((RecordKeyValue *) (key_val->val))->page_number);
		printf("\n\t\t\t\tnode->key_vals->val->slot_number = %i\n",((RecordKeyValue *) (key_val->val))->slot_number);
        	node->key_vals[i] = key_val;
        	table->header_page->b_tree->number_of_entries++;
	}

	return 0;
}


int commitIndexes(Indexes *indexes, FILE *fp) {
	// start from index, traverse through each indexes[] and in turn traverse through each indexNode[]
	int offset;
	int i;
	for(i = 0; i < indexes->number_of_indexes; ++i) {
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
	
}


int commitFormat(Format *format, FILE *fp) {

}


int commitTableHeader(Table *table, FILE *tp){
	
	// for each member of the table structure	
	fwrite(table->size , sizeof(table->size), sizeof(table->size), tp);
	fwrite(table->rid , sizeof(table->rid), sizeof(table->rid), tp);
	fwrite(table->increment , sizeof(table->increment), sizeof(table->increment), tp);
	fwrite(table->number_of_pages , sizeof(table->number_of_pages), sizeof(table->number_of_pages), tp);

	// fill out the rest of the page with 0's so the header page which holds the btree can start on its own page within the file
	int i;
	for(i = table->size; i < BLOCK_SIZE; i *= sizeof(int))
		fwrite(0, sizeof(0), sizeof(0), tp);
}


int commitHeaderPage(HeaderPage *header_page, FILE *tp){
	
	fwrite(header_page->space_available, sizeof(header_page->space_available), sizeof(header_page->space_available), tp);

	// TO DO, fwrite ORDER_OF_BTREE before writing tree
	fwrite(ORDER_OF_BTREE, sizeof(ORDER_OF_BTREE), 1, tp);
	
	// TO DO, fwrite number_of_nodes before
	fwrite(header_page->b_tree->number_of_entries, sizeof(header_page->b_tree->number_of_entries), 1, tp);	

	// TO DO, fwrite key type and value type
	fwrite(header_page->b_tree->key_type, sizeof(header_page->b_tree->key_type), 1, tp);
	fwrite(header_page->b_tree->value_type, sizeof(header_page->b_tree->value_type), 1, tp);

	// Serialize b-tree
	
	serializeTree(header_page->b_tree, header_page->b_tree->root, tp);	
}


// returns the position of where the record data can be inserted
int commitPage(Page *page, FILE *tp) {

}


int commitRecord(Record *record, FILE *tp, int offset) {

              return 0;
}


// FILE I/O
int commitTable(Table *table, FILE *fp) {

	commitTableHeader(table, fp);
	
	commitHeaderPage(table->header_page, fp);

	// for each page of the table (use multiple of BLOCK_SIZE i.e. 2nd (page[1]) page starts at BLOCK_SIZE, 3rd (page[2]) page starts at BLOCK_SIZE x 2)
	int pc, rc, i, j, offset;

        // for each page of the table
        for(i = 0, pc = 0; pc < table->number_of_pages && i < MAX_TABLE_SIZE; ++i){
                if(table->pages[i] == NULL)
                        continue;

		offset = commitPage(table->pages[i], fp);

                // for each record of that table
                for(j = 0, rc = 0; rc < table->pages[i]->number_of_records && j < MAX_RECORD_AMOUNT; ++j) {
                        if(table->pages[i]->records[j] == NULL)
                                continue;		                      
			
			commitRecord(table->pages[i]->records[j], fp, offset);
			 
                        ++rc;
                }

                ++pc;
        }

     
				
	return 0;	
}







// FILE FUNCTIONALITY

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


int fileExists(char *filename)
{
	struct stat   buffer;   
  	return (stat (filename, &buffer) == 0);
}


int openFile(FILE *fp, char *path_to_file, char *mode){
	if((fp = fopen(path_to_file, mode))==NULL) {
 		printf("\nError in Opening File %s\n", path_to_file);
  		return -1;
  	}

	return 0;
}



// INDEX FILE


// DATABASE

// opens folder if exists, creates folder if does not exist
int createFolder(char *database_name) {
	// get path to database file and check if the database exist, create if not
	char database_path[50];
	strcat(database_path, "data/");
	strcat(database_path, database_name);
	printf("\n\t\t\t\t database_path = %s\n", database_path);
	mkdir(database_path, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
}



int deleteFolder(char *folder_name) {
	// delete all files within database folder
	char full_command[100] = "rm -r ";
	char path[50] = "data/";
	strcat(path, folder_name);
	strcat(full_command, path);
	//strcat(full_command, "/");
	system(full_command);
	printf("\n\t\t\t\tFull command = %s\n", full_command);
	
	// delete folder itself
	return rmdir(path);	
}


