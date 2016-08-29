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




/**************************************************************** RECORD FUNCTIONALITY **************************************************************************/

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

	// set primary key of the new record	
	record->rid = table->rid++;	

	// insert record into that page
	page->records[page->record_position++] = record;	
	page->number_of_records++;

	// reduce space available of the page where the record is inserted
	page->space_available -= record->size_of_record;	

	return 0;	
}


// NOTE: the slot_array[slot_number] still points to the record in memory in a rollback is requested
int deleteRow(Table *table, int page_number, int slot_number){
	
	// get the record to be deleted
	Record *record = table->pages[page_number]->records[slot_number];

        // delete the records associated b-tree entry in the table b-tree
	btree_delete_key(table->header_page->b_tree, table->header_page->b_tree->root, &record->rid);
	
	// delete the records associated b-tree entries for each index of that table
	// for each index of the table, delete their associated index key-value pairs
	int i;
	char buffer[50];	// stores the result of getColumnData
	Index *index;
	for(i = 0; i < table->indexes->number_of_indexes; ++i){
		index = table->indexes->indexes[i];
		getColumnData(record, index->index_name, buffer, table->format);	// get the key
		btree_delete_key(index->b_tree, index->b_tree->root, buffer);		// delete the key-value pair from the indexes b-tree
	}
	
	// more space is available now the record has been deleted
	table->pages[page_number]->space_available += record->size_of_record;
	table->pages[page_number]->number_of_records--;

	// free the record
	free(record);

	// set the pages record array associated with record to NULL
        table->pages[page_number]->records[slot_number] = NULL;	

	// check if no records left on the page
	/* if there is no records then the page can be freed. Prevents this page being commited to disk */
	if(table->number_of_pages > 1 && table->pages[page_number]->space_available == BLOCK_SIZE) {
		free(table->pages[page_number]);
		table->pages[page_number] = NULL;
		table->number_of_pages--;
	}

	return 0;
}


// returns a single record where field equals value
Record * searchRecord(Table *table, char *field, char *value){
	
	Index *index = hasIndex(field, table);

	printf("\nsequential search\n");
	// check if the field is an index for quicker access
	if(index != NULL)
		return indexSearch(index, value, table); 	
	else // if not an index, perform sequential search for the record
		return sequentialSearch(field, value, table);
}


// search record based on its index
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


// searches through each pages array of records until it finds the target
Record * sequentialSearch(char *field, char *value, Table *table) {

	/*
		Some intermediate page entries may be NULL as a result of that pages records being deleted
		Therefore we need two counters, one that increments for each page slot and another that only increments when a nonon-NULL page is encountered
		The same applies to record entries. Records get deleted which means the record array will contain intermediate NULL entries
	*/
	int pc, rc, i, j;
	
	// for each page of the table
	for(i = 0, pc = 0; pc < table->number_of_pages && i < MAX_TABLE_SIZE; ++i){
		if(table->pages[i] == NULL)
			continue;
	
		// for each record of that table
		for(j = 0, rc = 0; rc < table->pages[i]->number_of_records && j < MAX_RECORD_AMOUNT; ++j) {
			if(table->pages[i]->records[j] == NULL)
				continue;

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
	
	int pos = locateField(table->format, field);

        if(strcmp(record->data[pos], value) == 0)			
		return 0;

	return -1;		
}


int getColumnData(Record *record, char *column_name, char *destination, Format *format) {
	Field **fields = format->fields;

	int pos = locateField(format, column_name);
	
	strcpy(destination, record->data[pos]);
		return 0;

	return -1;
}


int freeRecord(Record *record, int number_of_fields) {

	int i;
	
	for(i = 0; i < number_of_fields; ++i)
		free(record->data[i]);

	free(record->data);

	free(record);

	return 0;
}




/************************************************************** FIELD FUNCTIONALITY ******************************************************************************/

int createField(char *type, char *name, Format *format) {
	Field *field = malloc(sizeof(Field));
	strcpy(field->type, type);
	strcpy(field->name, name);
	format->fields[format->number_of_fields++] = field;
	return 0;
}


int locateField(Format *format, char *field) {
	int i;
        for(i = 0; i < format->number_of_fields; ++i) {
                if(strcmp(format->fields[i]->name, field) == 0){
			return i;		
                }
        }
}


/************************************************************** FORMAT FUNCTIONALITY *****************************************************************************/

// create a format struct from format "sql query"
int createFormat(Table *table, char *fields[], int number_of_fields) {
	Format *format = malloc(sizeof(Format));
	format->number_of_fields = 0;
	format->number_of_foreign_keys = 0;
			
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


int addForeignKey(Table *target_table, Table *origin_table, Field *field){

	target_table->format->foreign_keys[target_table->format->number_of_foreign_keys] = malloc(sizeof(ForeignKey));
	
	target_table->format->foreign_keys[target_table->format->number_of_foreign_keys]->field = field;
	target_table->format->foreign_keys[target_table->format->number_of_foreign_keys++]->table = origin_table;

	return 0;
}


int freeFormat(Format *format){
	int i;
	for(i = 0; i < format->number_of_fields; ++i)
		free(format->fields[i]);

	free(format);

	return 0;
}



/************************************************************* BTREE FUNCTIONALITY ******************************************************************************/

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





/*************************************************************** HEADER PAGE FUNCTIONALITY ***********************************************************************/

HeaderPage* createHeaderPage(Table *table) {
	HeaderPage* header_page = malloc(sizeof(HeaderPage));
	header_page->b_tree = createBtree("INT", "RECORD", sizeof(int), sizeof(RecordKeyValue));
       	header_page->space_available = BLOCK_SIZE - sizeof(header_page->space_available); // everytime we add a new b-tree node we increase the size by that node
	table->header_page = header_page;
	return header_page;
}


int freeHeaderPage(HeaderPage *headerPage) {
	// destroy btree
	btree_destroy(headerPage->b_tree);

	// free page
	free(headerPage);

	return 0;
}



/****************************************************************** PAGE FUNCTIONALITY **************************************************************************/

Page* createPage(Table *table) {
        Page *page = malloc(sizeof(Page));
        page->number = table->page_position;
	page->number_of_records = 0;
	page->record_position = 0;
	page->space_available = BLOCK_SIZE - sizeof(page->number) - sizeof(page->number_of_records) - sizeof(page->record_position) - sizeof(page->space_available) - (MAX_RECORD_AMOUNT * sizeof(unsigned long)); // last one is slot array
	table->pages[table->page_position++] = page;
	table->number_of_pages++;	
	return page;
}


int freePage(Page *page) {
	int rc, i;	
	for(i = 0; rc < page->number_of_records && i < MAX_RECORD_AMOUNT; ++i) {
		if(page->records[i] != NULL) {
			free(page->records[i]); //freeRecord(page->records[i]);
			++rc;
		}
	}

	free(page);

	return 0;
}



/************************************************************** RECORDKEY FUNCTIONALITY *************************************************************************/

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





/***************************************************************** INDEXES FUNCTIONALITY *************************************************************************/

// Indexes represent the index file
Indexes * createIndexes(Table *table) {
	Indexes *indexes = malloc(sizeof(Indexes));	
	indexes->space_available = MAX_INDEX_SIZE;
	indexes->number_of_indexes = 0;
	indexes->size = sizeof(indexes->space_available) + sizeof(indexes->size) + sizeof(indexes->number_of_indexes) + sizeof(indexes->indexes);
	table->indexes = indexes;	
	return indexes;
}


int freeIndexes(Indexes *indexes){
	int i;	
	for(i = 0; i < indexes->number_of_indexes; ++i)
		freeIndex(indexes->indexes[i]);

	free(indexes);		

	return 0;
}



/****************************************************************** INDEX FUNCTIONALITY **************************************************************************/
// pass the key size to the function. value size will be int
Index * createIndex(char *index_name, Table *table) {
	Index *index = malloc(sizeof(Index));
	
	strcpy(index->index_name, index_name);

	// get index name (key) type
	char key_type[50];

	int pos = locateField(table->format, index_name);

	strcpy(key_type, table->format->fields[pos]->type);			

	int key_size = getSizeOf(key_type);	
	
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


// returns index reference if field is an index in table, NULL otherwise
Index * hasIndex(char *field, Table *table) {
	int i;
	for(i = 0; i < table->indexes->number_of_indexes; ++i) {	
                if(strcmp(field, table->indexes->indexes[i]->index_name) == 0) 
			return table->indexes->indexes[i];
	}

	return NULL;
}


int freeIndex(Index *index) {
	btree_destroy(index->b_tree);
		
	free(index);
}




/****************************************************************** INDEX KEY FUNCTIONALITY **********************************************************************/
IndexKey * createIndexKey(char * key, int value) {
	
	IndexKey *indexKey = malloc(sizeof(IndexKey));

	indexKey->key = key;
	indexKey->value = value;
	indexKey->size_of_key = sizeof(indexKey->key) + sizeof(indexKey->value) + sizeof(indexKey->size_of_key);

	return indexKey;
}


int insertIndexKey(IndexKey *indexKey, Index *index) {
	bt_key_val * key_value = malloc(sizeof(key_value));
	key_value->key = malloc(strlen(indexKey->key) * sizeof(indexKey->key[0]) + 1);
	strcpy(key_value->key, indexKey->key);
	
	key_value->val = malloc(sizeof(int));
	* (int *)key_value->val = indexKey->value;
	btree_insert_key(index->b_tree, key_value);	

	if(strcmp(index->b_tree->key_type, "VARCHAR") == 0) {
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





/****************************************************************** TABLE FUNCTIONALITY *************************************************************************/

Table * createTable(char *table_name) {
	BLOCK_SIZE = getpagesize();

	Table *table = malloc(sizeof(Table));
	table->rid = 0;
	table->increment = 10;		
	table->number_of_pages = 0; // we need to use number_of_pages as an index so we set it to 0
	table->page_position = 0;
	table->record_type = VARIABLE_LENGTH; 
	table->size = sizeof(table->size) + sizeof(table->rid) + sizeof(table->increment) + sizeof(table->number_of_pages) + sizeof(table->page_position) + sizeof(table->record_type);
	createHeaderPage(table);
	createPage(table);
	createIndexes(table);
	return table;
}


int deleteTable(Table *table) {
	int pc, i;
	
	freeHeaderPage(table->header_page);
	
	 printf("\nbefore for loop\n");
	// for each page of the table
	for(i = 0, pc = 0; pc < table->number_of_pages && i < MAX_TABLE_SIZE; ++i){
		 printf("\n%d\n", i);
		if(table->pages[i] != NULL) {
			freePage(table->pages[i]);
			++pc;
		}
	}

	printf("\nbefore free format\n");
	freeFormat(table->format);

	printf("\nbefore free indexes\n");
	freeIndexes(table->indexes);

	free(table);

	// TO DO
	// insert into an array the map_page position of this deleted table so a callback request can be completed

	// TO DO
        // notify {x} when committing to remove the table from memory
        // make committing actually removes the table from disk
}






/***************************************************** SERIALIZATION/DESERIALIZATION *****************************************************************************/
/*
        The tree itself is represented by flattening it in prefix order. Each node is defined either to have children or not to have children. If a node is defined not to have children, the next physically succeeding node is a sibling. If a node is defined to have children, the next physically succeeding node is its first child. Additional children are represented as siblings of the first child. A chain of sibling entries is terminated by a null node.
*/

int serializeHeader(bt_node *node, FILE *fp) {

	// number of key-value pairs
	fwrite(&node->nr_active, sizeof(node->nr_active), 1, fp);

	// leaf node or not
	fwrite(&node->leaf, sizeof(node->leaf), 1, fp);

	// number of children
	fwrite(&node->level, sizeof(node->level), 1 , fp);

	return 0;
}


int serializeTree(btree *btree, bt_node * node, FILE *fp){

	printf("\n\t\t\t\tIn serialize\n");
	
	int total = 0;

	// signal starting new node
	total += fwrite(&STARTING_NODE_MARKER, sizeof(STARTING_NODE_MARKER), 1, fp);

	serializeHeader(node, fp);

        // serialize each key value pair of this node
        int i;
        for(i = 0; i < node->nr_active; ++i) {
               	
                if(strcmp(btree->value_type, "RECORD") == 0) {
			total += fwrite((int *) node->key_vals[i]->key, sizeof(node->key_vals[i]->key), 1, fp);
		        total += fwrite(&((RecordKeyValue *) (node->key_vals[i]->val))->page_number, sizeof(((RecordKeyValue *) (node->key_vals[i]->val))->page_number), 1, fp);
			total += fwrite(&((RecordKeyValue *) (node->key_vals[i]->val))->slot_number, sizeof(((RecordKeyValue *) (node->key_vals[i]->val))->slot_number), 1, fp);
                } else {
                 	if(strncmp(btree->key_type, "CHAR(", strlen("CHAR(")) == 0) {
				total += fwrite(node->key_vals[i]->key, (unsigned int) btree->key_size * sizeof(char), 1, fp);
			 } else if (strcmp(btree->key_type, "VARCHAR") == 0) {
				// fwrite that value in
				total += fwrite(&node->key_vals[i]->key_length, sizeof(node->key_vals[i]->key_length), 1, fp);				
				// fwrite the the data up to size			
				total += fwrite(node->key_vals[i]->key, node->key_vals[i]->key_length * sizeof(char), 1, fp);
			}
	
                        total += fwrite((int *)node->key_vals[i]->val, sizeof(node->key_vals[i]->val), 1, fp);			
                }
        }

	// signal end node
	total += fwrite(&ENDING_NODE_MARKER, sizeof(ENDING_NODE_MARKER), 1, fp);

        // if the node has children
        if(node->leaf == false){
		// for each child node of the current node
                for(i = 0 ; i < node->nr_active + 1; i++)
                        total += serializeTree(btree, node->children[i], fp);
	}

	return total;
}


bt_node * deserializeTree(FILE *fp, char *tree_type, Table *table) {

	
	bt_node *node = (bt_node *)mem_alloc(sizeof(bt_node));
	node->next = NULL;

	char start = 0;
	fread(&start, sizeof(start), 1, fp);
	
	fread(&node->nr_active, sizeof(node->nr_active), 1, fp);
	node->key_vals = (bt_key_val **)mem_alloc(2*ORDER_OF_BTREE*sizeof(bt_key_val*) - 1);

	fread(&node->leaf, sizeof(node->leaf), 1, fp);

	fread(&node->level, sizeof(node->level), 1, fp);        	

	if(strcmp(tree_type, "TABLE") == 0)
		deserializeTableTree(table, fp, node, node->nr_active);	
	else // its an index tree
		deserializeIndexTree(table, tree_type, fp, node, node->nr_active);

	char end = 0;
	fread(&end, sizeof(end), 1, fp);

	if(node->leaf == false) {
		node->children = (bt_node **)mem_alloc(2*ORDER_OF_BTREE*sizeof(bt_node*));   		
		int i;
		for(i = 0; i < node->nr_active + 1; ++i)
			node->children[i] = deserializeTree(fp, tree_type, table);  
	}

	return node;			
}


int deserializeIndexTree(Table *table, char *index_name, FILE *fp, bt_node *node, int number_of_entries){

	
	// get index
	Index *index = getIndex(index_name, table);

	printf("\nIN SERIALIZE index->b_tree->key_type = %s\n", index->b_tree->key_type);
	
	int i;	
	for(i = 0; i < number_of_entries; ++i) {		
		bt_key_val *key_val = malloc(sizeof(key_val));
	
		if (key_val == NULL) {
        		printf("Out of memory\n");
        		exit(-1);
    		}
	
		printf("\nIN SERIALIZE\n");	
	
		if(strcmp(index->b_tree->key_type, "VARCHAR") == 0 )	{
			key_val->key_length = 0;
			//fread(&(key_val->key_length), sizeof(key_val->key_length), 1 , fp);
			unsigned int demo = 0;
			fread(&demo, sizeof(demo), 1 , fp);
			key_val->key_length = demo;
			key_val->key = malloc((key_val->key_length * sizeof(char))+ 1);
			fread(key_val->key, key_val->key_length * sizeof(char), 1, fp);
			printf("\nIN SERIALIZE %s\n", key_val->key);
		} else {
			key_val->key = malloc((unsigned int) index->b_tree->key_size * sizeof(char) + 1);
			fread(key_val->key, (unsigned int) index->b_tree->key_size * sizeof(char), 1, fp);
		}
	       		
        	key_val->val = malloc(sizeof(int));
		fread(key_val->val, sizeof(key_val->val), 1, fp);
         	
        	node->key_vals[i] = key_val;
        	index->b_tree->number_of_entries++;
	}
	
	return 0;
}


int deserializeTableTree(Table *table, FILE *fp, bt_node *node, int number_of_entries) {

	int i;
	for(i = 0; i < number_of_entries; ++i) {

		bt_key_val *key_val = malloc(sizeof(key_val));

        	key_val->key = malloc(sizeof(int));
 		fread(key_val->key, sizeof(key_val->key), 1, fp); 
	
        	key_val->val = malloc(sizeof(RecordKeyValue));
		fread(&((RecordKeyValue *) (key_val->val))->page_number, sizeof(((RecordKeyValue *) (key_val->val))->page_number), 1, fp);
		fread(&((RecordKeyValue *) (key_val->val))->slot_number, sizeof(((RecordKeyValue *) (key_val->val))->page_number), 1, fp);
         	node->key_vals[i] = key_val;
        	table->header_page->b_tree->number_of_entries++;
	}

	return 0;
}




/****************************************************************** OPEN *****************************************************************************************/

/*
	Open table and import data into structure
	Close the file and perform all operations on the struct Table rather then the original file
	When the user is finished editing the table, the commit it to memory using an almost identical procedure
*/

int openTableHeader(FILE *tp, Table *table) {
	
        // for each member of the table structure
        fread(&table->size , sizeof(table->size), 1, tp);
        printf("\n\t\t\t\tOPEN: table->size = %d\n", table->size);
        fread(&table->rid , sizeof(table->rid), 1, tp);
        printf("\n\t\t\t\tOPEN: table->rid = %d\n", table->rid);
        fread(&table->increment , sizeof(table->increment), 1, tp);
        printf("\n\t\t\t\tOPEN: table->incremenet = %d\n", table->increment);
        fread(&table->number_of_pages , sizeof(table->number_of_pages), 1, tp);
        printf("\n\t\t\t\tOPEN: table->number_of_pages = %d\n", table->number_of_pages);
	fread(&table->page_position , sizeof(table->page_position), 1, tp);
        printf("\n\t\t\t\tOPEN: table->page_position = %d\n", table->page_position);
	fread(&table->record_type , sizeof(table->record_type), 1, tp);
	printf("\n\t\t\t\tOPEN: table->record_type = %d\n", table->record_type);

	// set the file pointer to the next page
	fseek(tp, BLOCK_SIZE, SEEK_SET);
	printf("\n\t\t\t\t\tftell = %d\n", ftell(tp));
}


Field * openField(FILE *fp) {
	Field *field = malloc(sizeof(Field));

	fread(&field->size, sizeof(field->size), 1, fp);

	int type_size = 0;
	fread(&type_size, sizeof(type_size), 1, fp);
	fread(field->type, type_size, 1, fp);

	int name_size = 0;
	fread(&name_size, sizeof(name_size), 1, fp);
      	fread(field->name, name_size, 1, fp);

	return field;
}


Format * openFormat(FILE * fp) {
	Format *format = malloc(sizeof(Format));

	fread(&format->number_of_fields, sizeof(format->number_of_fields), 1, fp);
	printf("\n\t\t\t\tformat->number_of_fields = %d\n", format->number_of_fields);
        fread(&format->format_size, sizeof(format->format_size), 1, fp);
	printf("\n\t\t\t\tformat->size = %d\n", format->format_size);

	//format->fields = (Field *) malloc(format->number_of_fields * sizeof(Field *));

        int i;
        for(i = 0; i < format->number_of_fields; ++i)
                format->fields[i] = openField(fp);

	return format;
}


Index * openIndex(FILE *fp, Table *table) {
	Index *index = malloc(sizeof(Index));
	
	int name_size = 0;
	fread(&name_size, sizeof(name_size), 1, fp);
	printf("\n\t\t\t\tname_size = %d\n", name_size);
	fread(index->index_name, name_size, 1, fp);
	printf("\n\t\t\t\tindex->index_name = %s\n", index->index_name);
        fread(&index->header_size, sizeof(index->header_size), 1, fp);	
	printf("\n\t\t\t\tindex->header_size = %d\n", index->header_size);
        fread(&index->btree_size, sizeof(index->btree_size), 1, fp);
	printf("\n\t\t\t\tindex->btree_size = %d\n", index->btree_size);


	return index;
}


int openIndexes(FILE * fp, Table *table) {
	Indexes *indexes = malloc(sizeof(Indexes));

	fread(&table->indexes->size, sizeof(table->indexes->size), 1, fp);
        fread(&table->indexes->space_available, sizeof(table->indexes->space_available), 1, fp);
        fread(&table->indexes->number_of_indexes, sizeof(table->indexes->number_of_indexes), 1, fp);
printf("\nnumber_of_indexes = %d, table->indexes->size = %d\n", table->indexes->number_of_indexes, table->indexes->size);	
	//indexes->indexes = malloc(indexes->number_of_indexes * sizeof(Index *));

	// for each index (starts at new page)
	int i;
	int offset = 0;
        for(i = 0; i < table->indexes->number_of_indexes; ++i) {
		offset += BLOCK_SIZE;
		fseek(fp, offset, SEEK_SET);
                table->indexes->indexes[i] = openIndex(fp, table); 
		
		int key_length = 0;
		char key_type[50];
		fread(&key_length, sizeof(key_length), 1, fp);
		fread(key_type, key_length, 1, fp);
		table->indexes->indexes[i]->b_tree = createBtree(key_type, "INT", key_length, sizeof(int));

		printf("\nBEFORE DESERIALIZE\n");
    		table->indexes->indexes[i]->b_tree->root = deserializeTree(fp, table->indexes->indexes[i]->index_name, table);
	}

	return 0;
}


int openHeaderPage(FILE *fp, Table *table) {

	printf("\n\t\t\t\tftell() = %d\n", ftell(fp));

      	printf("\n\t\t\t\tCreating Page\n");
        fread(&table->header_page->space_available, sizeof(table->header_page->space_available), 1, fp);
	printf("\n\t\t\t\ttable->header_page->space_available = %d\n", table->header_page->space_available);

	printf("\n\t\t\t\tCreating B-tree\n");
	table->header_page->b_tree = createBtree("INT", "RECORD", sizeof(int), sizeof(RecordKeyValue));
            
	printf("\n\t\t\t\tDeserializing Tree\n");
        // create bt_node
        table->header_page->b_tree->root = deserializeTree(fp, "TABLE", table);

	
	
	return 0;
}


Record * openRecord(FILE * tp, Format *format, int record_type) {
	Record *record = malloc(sizeof(Record));

	printf("\n\t\t\t\tSTART OF OPEN RECORD = %d, number_of_fields %d\n", ftell(tp), format->number_of_fields);
	
	fread(&record->rid, sizeof(record->rid), 1, tp);
	printf("\n\t\t\t\trecord->rid = %d\n", record->rid);
        //fread(&record->number_of_fields, sizeof(record->number_of_fields), 1, tp);
	//printf("\n\t\t\t\trecord->number_of_fields = %d\n", record->number_of_fields);
        fread(&record->size_of_data, sizeof(record->size_of_data), 1, tp);
        printf("\n\t\t\t\trecord->size_of_data = %d\n", record->size_of_data);
	fread(&record->size_of_record, sizeof(record->size_of_record), 1, tp);
	printf("\n\t\t\t\trecord->size_of_record = %d\n", record->size_of_record);

	// allocate data
	record->data = malloc(format->number_of_fields * sizeof(char *));

        int i;
        for(i = 0; i < format->number_of_fields; ++i) {
                if(record_type == FIXED_LENGTH) {		
			int size = getSizeOf(format->fields[i]->type);
                        record->data[i] = malloc(size);
			fread(record->data[i], size, 1, tp);
		} else {
                        int len = 0;
                        fread(&len, sizeof(len), 1, tp);               // read the length of the data column
			record->data[i] = malloc(len + 1);
                        fread(record->data[i], len + 1, 1, tp);        // read the actual column data
                }

		printf("\n\t\t\t\trecord->data[i] = %s\n", record->data[i]);
        }

	printf("\n\t\t\t\tSTART OF OPEN RECORD = %d\n", ftell(tp));

        return record;
}


int openPages(FILE * tp, Table * table) {

	int i, j, rc;
	int offset = BLOCK_SIZE;
	for(i = 0; i < table->number_of_pages; ++i) {

		printf("\n\n\n");

		offset += BLOCK_SIZE;	
		fseek(tp, offset, SEEK_SET); // set the file pointer to the following page				
		printf("ftell() = %d, offset = %d", ftell(tp), offset);
	
		Page *page = malloc(sizeof(Page));

	        fread(&page->number, sizeof(page->number), 1, tp);
		printf("\n\t\t\t\tpage->number = %d\n", page->number);
        	fread(&page->number_of_records, sizeof(page->number_of_records), 1, tp);
		printf("\n\t\t\t\tpage->number_of_records = %d\n", page->number_of_records);
		fread(&page->record_position, sizeof(page->record_position), 1, tp);
		printf("\n\t\t\t\tpage->record_position = %d\n", page->record_position);
        	fread(&page->space_available, sizeof(page->space_available), 1, tp);
		printf("\n\t\t\t\tpage->space_available = %d\n", page->space_available);

		// seek to slot array 
		// starts at offset - space_available - len of slot_array (MAX_RECORD_AMOUNT * sizeof(fpos_t))
		printf("\n\t\t\t\t- SLOT ARRAY = %d\n", (MAX_RECORD_AMOUNT * sizeof(unsigned long)));
		int seek_position = (offset + BLOCK_SIZE) - (MAX_RECORD_AMOUNT * sizeof(unsigned long));
		printf("\n\t\t\t\tseek_position = %d\n", seek_position);
		fseek(tp, seek_position, SEEK_SET);
		printf("\n\t\t\t\tftell() = %d\n", ftell(tp));
		fread(&page->slot_array, sizeof(unsigned long), MAX_RECORD_AMOUNT, tp);

		int k;
		for(k = 0; k < MAX_RECORD_AMOUNT; ++k)
			printf("\n\t\t\t\tslot_array[%d] = %d\n", k, page->slot_array[k]);
	
		table->pages[i] = page;
		if(page == NULL)
			continue;
	
        	// for each record of that page
                for(j = 0, rc = 0; rc < table->pages[i]->number_of_records && j < MAX_RECORD_AMOUNT; ++j) {

                        if(table->pages[i]->slot_array[j] == 0)
                                continue;

                        printf("\n\t\t\t\t\tBefore Opening Record\n");
			fseek(tp, table->pages[i]->slot_array[j], SEEK_SET);
                     	Record *record = openRecord(tp, table->format, table->record_type);
			table->pages[i]->records[j] = record;
		
                        ++rc;
                }

	}			

}


Table *openTable(char *table_name, char *database_name) {
	
	char path_to_table[50];
        getPathToFile(".csd", table_name, database_name, path_to_table);

        char path_to_index[50];
        getPathToFile(".csi", table_name, database_name, path_to_index);

        char path_to_format[50];
        getPathToFile(".csf", table_name, database_name, path_to_format);

        // declare file streams for each file
        FILE *tp, *ip, *fp;

        // set the mode based on whether the table already exists
        char mode[4];
        if(fileExists(path_to_table) == 1)
                strcpy(mode, "rb+");
        else
               	return -1;

        // connect the file streams to each file
        tp = fopen(path_to_table, mode);
        ip = fopen(path_to_index, mode);
        fp = fopen(path_to_format, mode);

	printf("\n\t\t\t\tCreating Table, mode = %s\n", mode);
	// reference mapped data into abstract structs 
	Table *table = createTable(table_name); 
 
	printf("\n\t\t\t\tOpening Table Header\n");
	openTableHeader(tp, table);

	printf("\n\t\t\t\t\tftell = %d\n", ftell(tp));

	printf("\n\t\t\t\tOpening Format\n");
	table->format = openFormat(fp);

	printf("\n\t\t\t\tOpening Indexes\n");
	openIndexes(ip, table);

	printf("\n\n\n");

	printf("\n\t\t\t\tftell() = %d\n", ftell(tp));
	printf("\n\t\t\t\tOpening Header Page\n");
	openHeaderPage(tp, table);

	printf("\n\n\n");

	printf("\n\t\t\t\tftell() = %d\n", ftell(tp));
	printf("\n\t\t\t\tOpening Pages\n");
	openPages(tp, table);
	
	return table;
}




/****************************************************************** COMMITS **************************************************************************************/

// HELPER METHODS
 
int fillPage(FILE *fp, int start, int end) {
	// fill the rest of the page with 0's
	int i;
	for(i = start; i < end; i += sizeof(FILLER))
		fwrite(&FILLER, sizeof(FILLER), 1, fp);
}


int commitTableHeader(Table *table, FILE *tp){

	// for each member of the table structure	
	fwrite(&table->size, sizeof(table->size), 1, tp);
	printf("\n\t\t\t\ttable->size = %d\n", table->size);
	fwrite(&table->rid, sizeof(table->rid), 1, tp);
	printf("\n\t\t\t\ttable->rid = %i\n", table->rid);
	fwrite(&table->increment, sizeof(table->increment), 1, tp);
	printf("\n\t\t\t\ttable->incremenet = %d\n", table->increment);
	fwrite(&table->number_of_pages , sizeof(table->number_of_pages), 1, tp);
	printf("\n\t\t\t\ttable->number_of_pages = %d\n", table->number_of_pages);
	fwrite(&table->page_position , sizeof(table->page_position), 1, tp);
	printf("\n\t\t\t\ttable->page_position = %d\n", table->page_position);
	fwrite(&table->record_type , sizeof(table->record_type), 1, tp);
	printf("\n\t\t\t\ttable->record_type = %d\n", table->record_type);

	// fill out the rest of the page with 0's so the header page which holds the btree can start on its own page within the file
	printf("\n\t\t\t\t\tftell = %d\n", ftell(tp));
	printf("\n\t\t\t\t\tBLOCK_SIZE = %d\n", BLOCK_SIZE);
	fillPage(tp, table->size, BLOCK_SIZE);
	printf("\n\t\t\t\t\tftell = %d\n", ftell(tp));
}


int commitField(Field *field, FILE *fp) {	
	fwrite(&field->size, sizeof(field->size), 1, fp);
	int type_size = strlen(field->type);
	fwrite(&type_size, sizeof(type_size), 1, fp);
	fwrite(field->type, strlen(field->type), 1, fp);
	int name_size = strlen(field->name);
	fwrite(&name_size, sizeof(name_size), 1, fp);
      	fwrite(field->name, strlen(field->name), 1, fp);
}


int commitFormat(Format *format, FILE *fp) {
	fwrite(&format->number_of_fields, sizeof(format->number_of_fields), 1, fp);
        fwrite(&format->format_size, sizeof(format->format_size), 1, fp);

	int i;
	for(i = 0; i < format->number_of_fields; ++i)
        	commitField(format->fields[i], fp);
}


int commitIndex(Index *index, FILE *fp) {

	int name_size = strlen(index->index_name);
	fwrite(&name_size, sizeof(name_size), 1, fp);
	fwrite(index->index_name, strlen(index->index_name), 1, fp);
      	fwrite(&index->header_size, sizeof(index->header_size), 1, fp);
	printf("\nindex->header size = %d\n", index->header_size);
	fwrite(&index->btree_size, sizeof(index->btree_size), 1, fp);
	printf("\nindex->btree size = %d\n", index->btree_size);
	int key_length = strlen(index->b_tree->key_type);
	fwrite(&key_length, sizeof(key_length), 1, fp);
	fwrite(&index->b_tree->key_type, key_length, 1, fp);
        printf("\nindex->btree key_type = %s\n", index->b_tree->key_type);

	serializeTree(index->b_tree, index->b_tree->root, fp);
}


int commitIndexes(Indexes *indexes, FILE *fp) {

	printf("\nIndexes ftell() = %d, ndexes->number_of_indexes = %d, indexes->size = %d\n", ftell(fp), indexes->number_of_indexes, indexes->size);
	fwrite(&indexes->size, sizeof(indexes->size), 1, fp);
        fwrite(&indexes->space_available, sizeof(indexes->space_available), 1, fp);
	fwrite(&indexes->number_of_indexes, sizeof(indexes->number_of_indexes), 1, fp);
	
	// for each index where i equals a new page number
	int i;
	
	printf("\nIndexes ftell() = %d\n", ftell(fp));
	// for each index (starts at new page)
	for(i = 0; i < indexes->number_of_indexes; ++i) {
		fillPage(fp, ftell(fp) % BLOCK_SIZE, BLOCK_SIZE);		
		commitIndex(indexes->indexes[i], fp);		
	}

	return 0;
}


int commitHeaderPage(HeaderPage *header_page, FILE *tp){

	printf("\n\t\t\t\tftell() = %d, space_available = %d\n", ftell(tp), header_page->space_available);	
	fwrite(&header_page->space_available, sizeof(header_page->space_available), 1, tp);

	// Serialize b-tree
	serializeTree(header_page->b_tree, header_page->b_tree->root, tp);		

	printf("\n\t\t\t\tftell() = %d, ftell - BLOCK_SIZE = %d, sizeof FILLER = %d\n", ftell(tp), ftell(tp) % BLOCK_SIZE, sizeof(FILLER));	

	fillPage(tp, ftell(tp) % BLOCK_SIZE, BLOCK_SIZE);
	printf("\n\t\t\t\tftell() = %d\n", ftell(tp));
}


unsigned long commitRecord(Record *record, Format *format, FILE *tp, int record_type) {

	unsigned long position;
	fflush(tp);
	position = ftell(tp);

	printf("\n\t\t\t\tSTART OF COMMIT RECORD = %d\n", ftell(tp));
	// get the position of where the record is inserted before we insert the record
	fwrite(&record->rid, sizeof(record->rid), 1, tp);
       	//fwrite(&record->number_of_fields, sizeof(record->number_of_fields), 1, tp);
	fwrite(&record->size_of_data, sizeof(record->size_of_data), 1, tp);
	fwrite(&record->size_of_record, sizeof(record->size_of_record), 1, tp);       

	int i;
	for(i = 0; i < format->number_of_fields; ++i) {
		if(record_type == FIXED_LENGTH)
			fwrite(record->data[i], strlen(record->data[i]) + 1, 1, tp);
		else {
			int len = strlen(record->data[i]);
			fwrite(&len, sizeof(len), 1, tp);		// write the length of the data column
			fwrite(record->data[i], len + 1, 1, tp);	// write the actual column data
		}
			
	}

	printf("\n\t\t\t\tEND OF COMMIT RECORD = %d\n", ftell(tp));

        return position;
}


// returns the position of where the record data can be inserted
int commitPages(Table *table, FILE *tp) {
	
	// for each record in the slot array
	// for each page of the table (use multiple of BLOCK_SIZE i.e. 2nd (page[1]) page starts at BLOCK_SIZE, 3rd (page[2]) page starts at BLOCK_SIZE x 2)
	int pc, rc, i, j, offset;

        // for each page of the table
        for(i = 0, pc = 0; pc < table->number_of_pages && i < MAX_TABLE_SIZE; ++i) {
                
		if(table->pages[i] == NULL)
                        continue;
		printf("\n\n\n");

		printf("\nftell() = %d\n", ftell(tp));

		fwrite(&table->pages[i]->number, sizeof(table->pages[i]->number), 1, tp);
        	fwrite(&table->pages[i]->number_of_records, sizeof(table->pages[i]->number_of_records), 1, tp);       
		fwrite(&table->pages[i]->record_position, sizeof(table->pages[i]->record_position), 1, tp);
 		fwrite(&table->pages[i]->space_available, sizeof(table->pages[i]->space_available), 1, tp);
		
                // for each record of that page
                for(j = 0, rc = 0; rc < table->pages[i]->number_of_records && j < MAX_RECORD_AMOUNT; ++j) {
                        
			if(table->pages[i]->records[j] == NULL) {
				table->pages[i]->slot_array[j] = 0;
                                continue;		         
			}  
									
			printf("\n\t\t\t\tBefore Commit Record\n");
			unsigned long pos = commitRecord(table->pages[i]->records[j], table->format, tp, table->record_type);	
			
			table->pages[i]->slot_array[j] = pos;
			printf("\n\t\t\t\tRecord pos = %d\n", pos);
						 
                        ++rc;
                }

		fillPage(tp, ftell(tp) % BLOCK_SIZE, BLOCK_SIZE - (MAX_RECORD_AMOUNT * sizeof(unsigned long)));

		printf("\n\t\t\t\tCOMMIT seek position = %d\n", ftell(tp));
		fwrite(table->pages[i]->slot_array, sizeof(unsigned long), MAX_RECORD_AMOUNT, tp);

		/*
		// fill the rest of the page with 0s
                fillPage(tp, table->pages[i]->space_available);
		printf("\n\t\t\t\tftell() = %d, space_available = %d\n", ftell(tp), table->pages[i]->space_available);
		*/

                ++pc;
        }

}


// commits the entire table
int commitTable(Table *table, char *table_name, char *database_name) {

	printf("\nFUCK\n");
	char path_to_table[50];
        getPathToFile(".csd", table_name, database_name, path_to_table);

        char path_to_index[50];
        getPathToFile(".csi", table_name, database_name, path_to_index);

        char path_to_format[50];
        getPathToFile(".csf", table_name, database_name, path_to_format);

        // declare file streams for each file
        FILE *tp, *ip, *fp;

        // set the mode based on whether the table already exists
        char mode[4];
        if(fileExists(path_to_table) == 1)
                strcpy(mode, "rb+");
        else
                strcpy(mode, "wb+");

	printf("\n\t\t\t\tDICKHEADS, HELLO%s\n", mode);

        // connect the file streams to each file
        tp = fopen(path_to_table, mode);
        ip = fopen(path_to_index, mode);
        fp = fopen(path_to_format, mode);

	printf("\n\t\t\t\tBefore Commit Table Header\n");
	commitTableHeader(table, tp);
	
	// TO DO, open format file
	printf("\n\t\t\t\tBefore Commit Format\n");	
	commitFormat(table->format, fp);

	// TO DO, open Index file
	printf("\n\t\t\t\tBefore Commit Table Indexes\n");
	commitIndexes(table->indexes, ip);

	printf("\n\n\n");

	printf("\n\t\t\t\tftell() = %d\n", ftell(tp));	
	printf("\n\t\t\t\tBefore Commit Header_Page\n");
	commitHeaderPage(table->header_page, tp);

	printf("\n\n\n");
	printf("\n\t\t\t\tftell() = %d\n", ftell(tp));	
	printf("\n\t\t\t\tBefore Commit Pages\n");
	commitPages(table, tp);	

	// close files
	fclose(tp);
        fclose(ip);
        fclose(fp);

			
	return 0;	
}




/***************************************************************** FILE FUNCTIONALITY ****************************************************************************/

// creates full path in string to file of choice
int getPathToFile(char *file_extension, char *table_name, char *database, char *destination) {
	
	destination[0] = 'd';
	destination[1] = 'a';
	destination[2] = 't';
	destination[3] = 'a';
	destination[4] = '/';
	
	int i, j;
	for(i = strlen("data/"), j = 0; i < strlen(database) + strlen("data/"); ++i, ++j) {
		destination[i] = database[j];
	}

	destination[i++] = '/';
	
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
	int result = (stat (filename, &buffer) == 0);
	printf("\n\t\t\t\t\tFILE EXISTS result = %d\n", result);
  	return result;
}


int openFile(FILE *fp, char *path_to_file, char *mode){
	if((fp = fopen(path_to_file, mode))==NULL) {
 		printf("\nError in Opening File %s\n", path_to_file);
  		return -1;
  	}

	return 0;
}



/******************************************************************* DATABASE ************************************************************************************/

// opens folder if exists, creates folder if does not exist
int createFolder(char *database_name) {
	// get path to database file and check if the database exist, create if not
	char database_path[50];
	strcat(database_path, "data/");
	strcat(database_path, database_name);
	printf("\n\t\t\t\t database_path = %s\n", database_path);
	mkdir(database_path, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
}


// delete a database folder
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


