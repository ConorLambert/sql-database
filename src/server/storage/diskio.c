/*
 I NEED : data[BLOCK_SIZE] in each struct
	DO I NEED : struct records or could I just iterate through the slot array
*/


#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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

struct Record {
        int _rid;
        int size_of_data; // size of record in bytes
	int size_of_record; // complete size of record including this field
	char *data;  
};

struct Record createRecord(char *data){
        struct Record record;
        strcpy(record.data, data);
	record._rid = 0;
        record.size = sizeof(data);
	record.size_of_record = sizeof(record._rid) + sizeof(record.size_of_data) + sizeof(record.size_of_record) + sizeof(record.data);
	return record;
}

int insertRecord(struct Record record, struct Table table) {
	record._id = table.rid++;	
	
	// get last page
	struct Page page = table.pages[table.number_of_pages - 1];

	// insert record into that page and increment record count
	page.records[page.number_of_records] = record;	

	// add slot for new record inserted
	page.slot_array[page.number_of_records++] = page.size - page.space_available - record.size_of_record;

	// return primary key of inserted record
	return table.rid;	
}

int commitRecord(struct Record record, struct Table table) {

        // get page of table where record is to be inserted
        int page_location = table.page_number * BLOCK_SIZE;

        // get path to table file
        char path_to_table[50]; // TO DO
        int fd = open(path_to_table, O_RDWR);

        // mapped the file to memory starting at page_location
        char *map_page = mmap((caddr_t)0, BLOCK_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED, fd, page_location);

        // get the insert location of the new record
        int space_available = map_page[PAGE_SPACE_AVAILABLE_BYTE];
        int insert_location = BLOCK_SIZE - space_available;

	// update record count
	map_page[NUMBER_OF_RECORDS_BYTE] = page.number_of_records;        

	// TO DO check there is enough room for this last record

        // SERIALIZE record at position insert_location

        // get address of new record got from mmap append

        // insert this address into next available slot

        return 0;
}


struct Record searchRecord(struct Table table, char *condition){
	// find node 
		// perform binary search on tree
		// if node found
			// get (page,slot) numbers 
		// else 
			// return NULL

	// TO DO variable length processing
	/*
		If the record is variable length then each field will need to be defined per record
		For fixed length only the field lengths need to be defined once for all records
		This means the record layout and thus processing of a variable length record will be different
		This may require a byte before each field which defines the size of the immediate proceeding field
	*/


	return NULL;
}




// PAGE FUNCTIONALITY

struct HeaderPage {
	int space_available;
	// TO DO Reference to B-Tree
};

struct HeaderPage createHeaderPage() {
	struct HeaderPage header_page;
	header_page.space_available = BLOCK_SIZE;
	return header_page;
}

struct Page {
	char number;
	int space_available;
	int number_of_records;
	void* slot_array[SLOT_SIZE]; 
	int record_type; // fixed or variable length
	struct Record records[MAX_RECORD_AMOUNT];
};

struct Page createPage(struct Table table) {
        struct Page page;
        page.number = table.number_of_pages;
        page.space_available = BLOCK_SIZE;
	page.number_of_records = 0;
	page.record_type = -1 // initialized to undefined
        return page;
}


void mapPages(struct Table table, char *map_table) {
	// get reference to header page
	char *map_table = mmap((caddr_t)0, BLOCK_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
	
	
}

int addSlot(char *address, struct Page page){
	page.slot_array[page.number_of_records] = address;
	return 0;	
}





// NODE FUNCTIONALITY

struct Node {
	int _rid;
	int page;
	int slot_number;
};

struct Node createNode(struct Page page, struct Record record) {
	struct Node node;
	node._rid = record._rid;  
	node.page = page.number;
	node.slot_number = page.number_of_records - 1;
	return node;
}

int insertNode(struct Node node) {

	// TO DO	
	// perform binary search
		// find insertion point
		// insert		
	return 0;
}

struct Node findNode(int rid) {
	return NULL;
}





// TABLE FUNCTIONALITY

struct Table {
	int size; 	// size of all records only i.e. excluding header files
	int rid;	// primary key, increases with each new record added
	int increment;  // how much the primay key increases each new insertion
	int number_of_pages; // total count for all pages of a table
	struct HeaderPage header_page;
	struct Page *pages[MAX_TABLE_SIZE];
	// TO DO Reference to B-TREE
};


/*
		
*/
struct Table createTable(char *table_name) {
	BLOCK_SIZE = getpagesize();

	struct Table table;
	table.size = 0;
	table.rid = 0;
	table.increment = 10;		
	table.number_of_pages = 0; // we need to us number_of_pages as an index so we set it to 0
	
	table.header_page = createHeaderPage();

	struct Page page = createPage(table);
	addPageToTable(page, table);
	
	return table;
}


/*
	Open table and import data into structure
	Close the file and perform all operations on the struct Table rather then the original file
	When the user is finished editing the table, the commit it to memory using an almost identical procedure
*/
struct Table openTable(char *table_name, char *database) {
	
	// concat database and table_name to get file path
	char *path_to_table = getPathToTable(table_name, database);

	// map entire table into memory for easy access
	char *map_table = mapTable(path_to_table);	
	
	// referebce mapped data into abstract structs 
	struct Table table = initializeTable(map_table);
 
	// close mapped file
	closeMap(map_table, fd);
	
	return table;
}


	char * getPathToTable(char *table, char *database) {
		int i;
		char path_to_table[50];
		for(i = 0; i < strlen(database); ++i) {
			path_to_table[i] = database[i];
		}
		path_to_table[i++] = '/';
		int j;
		for(j = 0; j < strlen(table_name); ++i, ++j) {
			path_to_table[i] = table_name[j];
		}

		path_to_table[i] = '\0';

		return path_to_table
	}


	char * mapTable(char * path_to_table) {
		int fd = open(path_to_table, O_RDWR);	
		char *map_table = mmap((caddr_t)0, BLOCK_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
		
		// find how many pages the table has
		int number_of_pages = map_table[NUMBER_OF_PAGES__BYTE];	

		// unmap the table and close file descriptor
		munmap(map_table, BLOCK_SIZE);
		close(fd);

		// map the table again fully with all pages
		map_table = mmap((caddr_t)0, (BLOCK_SIZE * number_of_pages), PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);

		return map_table;	
	}


	struct Table table initializeTable(char *map_table) {
		table.size = map_table[SIZE_BYTE];
		table.rid = map_table[HIGHEST_RID_BYTE];
		table.increment = map_table[INCREMENT_BYTE];
		table.number_of_pages = map_table[NUMBER_OF_PAGES_BYTE]; 
		table.header_page.space_available = map_table[SPACE_AVAILABLE_BYTE];
		// TO DO reference B-Tree

		// map each of the pages		
		
		// for each page		
		int i;
		for(i = 0; i < number_of_pages; ++i) {
			int page_offset = BLOCK_SIZE * i;
			struct Page page;
			page.number = map_table[PAGE_NUMBER_BYTE + page_offset];
			page.space_available = map_table[PAGE_SPACE_AVAILABLE_BYTE + page_offset];
			page.number_of_records = map_table[NUMBER_OF_RECORDS_BYTE + page_offset];
			page.record_type = map_table[RECORD_TYPE_BYTE + page_offset];
			

			// for each slot of this page
			int j;
			for(j = 0; j < SLOT_SIZE; ++j)
				page.slot_array[j] = map_table[(SLOT_ARRAY_BYTE + page_offset) + j];

			// for each record of this page
			
			// use the record length as an offset (in bytes)
			int record_length = 0;
			for(j = 0; j < page.number_of_records; ++j) {
				struct Record record;
				record._rid = map_table[(RECORD_BYTE + RID_BYTE) + record_length];
				record.size_of_data = map_table[(RECORD_BYTE + SIZE_OF_DATA_BYTE) + record_length];
				record.size_of_record = map_table[(RECORD_BYTE + SIZE_OF_RECORD_BYTE) + record_length];
				record.data = map_table[(RECORD_BYTE + DATA_BYTE) + record_length];
				record_length += record.size_of_record;
				page.records[j] = record;
			}		
				

			table.pages[i] = page;			
		}
			
		return table;
	}
	

	void closeMap(char *map_table, int fd) {
		munmap(map_table, BLOCK_SIZE);
		close(fd);
	}


// returns -1 if page cannot fit
int addPageToTable(struct Page page, struct Table table) {
	table.pages[table.number_of_pages++] = page;
}


int commitTable(char *table_name, struct Table table) {
	

	// mmap table into memory	
	
	// [TABLE_SIZE - CURRENT_MAX_RID - INCREMENT_AMOUNT - PAGE_NUMBER - SPACE_AVAILABLE]
	char data[BLOCK_SIZE];
	data[SIZE_BYTE] = table.size;
	data[RID_BYTE] = table.rid;
	data[INCREMENT_BYTE] = table.increment;
	data[PAGE_NUMBER_BYTE] =  table.header_page.number;
	data[HEADER_PAGE_AVAILABLE_BYTE] =  table.header_page.space_available;
		
	// TO DO: Commit each page of the table
	// for each page of the table (use multiple of BLOCK_SIZE i.e. 2nd page starts at BLOCK_SIZE, 3rd page starts at BLOCK_SIZE x 2)

	return 0;	
}







