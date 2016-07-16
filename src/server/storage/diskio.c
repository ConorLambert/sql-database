#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define BLOCK_SIZE 512
#define SLOT_SIZE 20 // how many records can the page hold TO DO should be a based on individual record size 

// MOVE THESE TO access
int rid = 0;	// primary key, increases with each new record added
int AUTO_INCREMENT = 10; // how much the primay key increases each new insertion

int page_number = 0; // total count for all pages of a table

struct Record {
        int _rid;
        char *data; // column data
        int size; // size of record in bytes
};

struct Record createRecord(char *data) {
        struct Record record;
        strcpy(record.data, data);
        record.size = sizeof(data) + strlen(data);
        record._rid = rid++;
        return record;
}

struct Page {
	int number;
	int space_available;
	int number_of_records;
	// last address position
	void* slot_array[SLOT_SIZE]; // TO DO: define size	
};

struct Page createPage() {
	struct Page page;
	page.number = page_number++;
	page.space_available = BLOCK_SIZE;
	page.number_of_records = 0;
	return page;
}

// node on the binary tree
struct Node {
	int _rid;
	int page;
	int slot_number;
};



int append(struct Record record) {
	
	// TO DO return address
	return 0;
}

int addSlot(char *address, struct Page page){
	page.slot_array[page.number_of_records] = address;
	return 0;	
}

int insertNode(struct Node node) {
	
	// perform binary search
		// find insertion point
		// insert		
	return 0;
}

struct Node createNode(struct Page page) {
	struct Node node;
	node._rid = rid + 1; // TO DO: AUTO-INCREMENT 
	node.page = page.number;
	node.slot_number = page.number_of_records;
	return node;
}

int addNode(struct Page page) {
	struct Node node = createNode(page);
	insertNode(node);
	return 0;
}





