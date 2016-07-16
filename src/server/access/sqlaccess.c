#include <stdio.h>
#include "sqlaccess.h"

int insert(struct Record record, int size, char *table){
	// map table.csd file into memory

	// append record to end of file 
		// TO DO something to do with mmap
		// IF FIXED LENGTH
			// SERIALIZE record at position last record plus record_length
		// IF VARIABLE LENGTH
			// DESERIALIZE last record
			// get length of the last record
			// SERIALIZE new record at address : last record + last_record_length 
	
	// get address of new record got from mmap append

	// insert this address into next available slot

	
}

int deleteRecord(char *field, int size, char *table) {
	return -1;
}

int update(char *field, int size, char *value, char *table) {	
	return -1;
}

char * select(char *condition, char *table) {
	return NULL;
}

int create(char *table_name) {
	return -1;
}

int drop(char *table) {
	return -1;
}

// TO DO
int alter() {}



