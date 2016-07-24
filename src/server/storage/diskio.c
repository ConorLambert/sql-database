/*
 I NEED : data[BLOCK_SIZE] in each struct
	DO I NEED : struct records or could I just iterate through the slot array
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/mman.h>
#include "diskio.h"

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

#define MAX_RECORD_AMOUNT 250 
 
#define SEPARATOR " "



// RECORD FUNCTIONALITY

/*
Record {
        int rid;
        int size_of_data; // size of record in bytes
	int size_of_record; // complete size of record including this field
	char *data;  
};
*/

Record * createRecord(char *data){
        Record *record = malloc(sizeof(Record));
	record->rid = 0;
        record->size_of_data = (strlen(data) + 1) * sizeof(char);
	record->data = malloc(record->size_of_data);
	strcpy(record->data, data);
	record->size_of_record = sizeof(record->rid) + sizeof(record->size_of_data) + sizeof(record->size_of_record) + sizeof(record->data);
	return record;
}

int insertRecord(Record *record, Page *page, Table *table) {
	record->rid = table->rid++;	

	// insert record into that page and increment record count
	// perform deep copy of record
	page->records[page->number_of_records] = record;	

	// add slot for new record inserted
	// the RValue returns an offset in bytes
	page->slot_array[page->number_of_records++] = BLOCK_SIZE - page->space_available - record->size_of_record;

	// return primary key of inserted record
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


Record searchRecord(Table *table, char *condition){
	// find node 
		// perform binary search on tree
		// if node found
			// get (page,slot) numbers 
		// else 
			// return NULL

	Record record;
	// TO DO variable length processing
	/*
		If the record is variable length then each field will need to be defined per record
		For fixed length only the field lengths need to be defined once for all records
		This means the record layout and thus processing of a variable length record will be different
		This may require a byte before each field which defines the size of the immediate proceeding field
	*/


	return record;
}




// PAGE FUNCTIONALITY

/*
struct HeaderPage {
	int space_available;
	// TO DO Reference to B-Tree
};
*/

HeaderPage* createHeaderPage() {
	HeaderPage* header_page = malloc(sizeof(header_page));
	header_page->space_available = BLOCK_SIZE;
	return header_page;
}

/*
struct Page {
	char number;
	int space_available;
	int number_of_records;
	void* slot_array[SLOT_SIZE]; 
	int record_type; // fixed or variable length
	struct Record records[MAX_RECORD_AMOUNT];
};
*/

Page* createPage(Table *table) {
        Page *page = malloc(sizeof(page));
        page->number = table->number_of_pages;
        page->space_available = BLOCK_SIZE;
	page->number_of_records = 0;
	page->record_type = -1; // initialized to undefined
        return page;
}





// NODE FUNCTIONALITY
/*
struct Node {
	int rid;
	int page;
	int slot_number;
};
*/

Node createNode(int rid, int page_number, int slot_number) {
	Node node;
	node.rid = rid;
	node.page_number = page_number;
	node.slot_number = slot_number;
    	return node;
}

int insertNode(Node *node) {

	// TO DO	
	// perform binary search
		// find insertion point
		// insert		
	return 0;
}

Node findNode(int rid) {
	Node node;
	
	return node;
;
}





// TABLE FUNCTIONALITY
/*
struct Table {
	int size; 	// size of all records only i.e. excluding header files
	int rid;	// primary key, increases with each new record added
	int increment;  // how much the primay key increases each new insertion
	int number_of_pages; // total count for all pages of a table
	struct HeaderPage header_page;
	struct Page *pages[MAX_TABLE_SIZE];
	// TO DO Reference to B-TREE
};
*/


Table* createTable(char *table_name) {
	BLOCK_SIZE = getpagesize();

	Table *table = malloc(sizeof(table));
	table->size = 0;
	table->rid = 0;
	table->increment = 10;		
	table->number_of_pages = 0; // we need to use number_of_pages as an index so we set it to 0
	
	table->header_page = createHeaderPage();

	Page *page = createPage(table);
	table->pages[table->number_of_pages++] = page;
	
	return table;
}


/*
	Open table and import data into structure
	Close the file and perform all operations on the struct Table rather then the original file
	When the user is finished editing the table, the commit it to memory using an almost identical procedure
*/
Table *openTable(char *table_name, char *database) {
	
	char path_to_table[50];

	// concat database and table_name to get file path
	getPathToTable(table_name, database, path_to_table);

	// map entire table into memory for easy access
	char *map_table = mapTable(path_to_table);	
	
	// referebce mapped data into abstract structs 
	Table *table = initializeTable(map_table);
 
	// close mapped file
	munmap(map_table, BLOCK_SIZE);
		
	return table;
}


	getPathToTable(char *table_name, char *database, char *destination) {
		int i;
		for(i = 0; i < strlen(database); ++i) {
			destination[i] = database[i];
		}
		destination[i++] = '/';
		int j;
		for(j = 0; j < strlen(table_name); ++i, ++j) {
			destination[i] = table_name[j];
		}

		destination[i] = '\0';
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
				page->records[j]->size_of_data = map_table[(RECORD_BYTE + SIZE_OF_DATA_BYTE) + record_length];
				page->records[j]->size_of_record = map_table[(RECORD_BYTE + SIZE_OF_RECORD_BYTE) + record_length];
				page->records[j]->data = malloc(page->records[j]->size_of_data);
				mapData(RECORD_BYTE + DATA_BYTE + record_length, page->records[j]->size_of_data, map_table, page->records[j]->data);  // map_table[(RECORD_BYTE + DATA_BYTE) + record_length];
				record_length += page->records[j]->size_of_record;
				// assign each member of page.records manually

			}		
	
			table->pages[i] = page;			
		}
			
		return table;
	}
		
		int mapData(int start_location, int record_size, char *map, char *destination) {
			int i, j;
			for(i = start_location, j = 0; i < record_size; ++i, ++j)
				destination[j] = map[i];
			return 0;
		}



// TO DO
int closeTable(Table *table) {
	// traverse through the entire table and free up the memory
}

int commitTable(char *table_name, Table *table, char *database_name) {
	
	// mmap table into memory	
	
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


// DATABASE
int createFolder(char *folder_name) {
	// mkdir
}







