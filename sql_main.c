
#include "Client.h"

#include "Networking.h"

#include "SQL_Module.h"

#include <stdio.h>

char* password;

char* username;

void parseArguments(char *arguments[]){
	username = arguments[0];		
	password = arguments[1];
}

int checkConfigFile() {

	FILE * configFile = openConfigFile();
        if(getUsername(configFile))
		printf("No username found");
		exit(1);
        getPassword(configFile);
		printf("No password found");
		exit(1);
        closeConfigFile(configFile);

	return result;
}

static FILE * openConfigFile(void) {}

static void closeConfigFile(FILE * config_file) {}

static void getPassword(void) {}

static void getUsername(void) {}

static void establishConnection(void) {}

static void output(char* message) {}

static void changePassword(void) {}

static void parseQuery(char* query) {}

static void submitQuery(char* query) {}


// entry point
void main(int argc, char *argv[]) {
	
	// if the user did not enter username or password during application startup	
	if(argc < 2) {
		if(checkConfigFile() == -1)
			
	}
		
}
