/*
        #include "Client.h"

        #include "Networking.h"

        #include "SQL_Module.h"
        */


#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <termios.h>
#include "../../libs/libcfu/src/cfuhash.h"

void changePassword(void) {}

void parseQuery(char* query) {}

void submitQuery(char* query) {}


typedef int bool;
#define true 1
#define false 0

char password[30];

char *username = NULL;

char *host = NULL;

cfuhash_table_t *arguments = NULL;

void argumentInit() {
	
	// setup hash table to hold arguments	
	arguments = cfuhash_new_with_initial_size(50); 
	cfuhash_set_flag(arguments, CFUHASH_FROZEN_UNTIL_GROWS);

	// add arguments to hash table
	cfuhash_put(arguments, "-u", username);
	cfuhash_put(arguments, "-h", host);
	cfuhash_put(arguments, "-p", password);
}

int checkConfigFile() {
/*
        FILE * configFile = openConfigFile();
        if(getUsername(configFile))
                printf("No username found");
                exit(1);
        getPassword(configFile);
                printf("No password found");
                exit(1);
        closeConfigFile(configFile);

        return result;
*/

return 0;
}

void acceptInput() {

        char line[256];
        char input[256];

        // infinite loop
        while(1) {
                // display prompt
                fputs(">> ", stderr);
                // accept input
                fgets(line, sizeof line, stdin);
                sscanf(line, "%s", input);
                // if input is "exit"
                if(strcmp(input, "exit") == 0)
                        return; // quit program
                else
                        fprintf(stdout, "Characters read : %d\n", strlen(line) - 1); // TO DO process input
        }
}

void startClient() {
	acceptInput();
}


char * promptPasswordEntry() {
	
	int ch = 0;
	int i;
	for(i = 0, ch = getch(); ch != '\n'; ++i) {
		password[i] = ch;
		ch = getch();
	}

	password[i] = '\0';

	// TEST
	printf("Password entered is %s\n", password);
}

int getch() {
    struct termios oldt, newt;
    int ch;
    tcgetattr(STDIN_FILENO, &oldt);
    newt = oldt;
    newt.c_lflag &= ~(ICANON | ECHO);
    tcsetattr(STDIN_FILENO, TCSANOW, &newt);
    ch = getchar();
    tcsetattr(STDIN_FILENO, TCSANOW, &oldt);
    return ch;
}


char * testTriggerPasswordEntry() {
        //char input[30];
        //fputs("Please enter your password: ", stdout);
        //fgets(input, sizeof input, stdin);
        sscanf("somepassword", "%s", password);
        return password;
}


int isOption(char *argument) {

	printf("argument %s\n", argument);
	
	if(cfuhash_exists(arguments, argument)) {
		puts("Returning 0");
		return 0;
	}
	
	return -1;
}


/*
        Input arguments have a pattern of -option argument -option argument
        For example -u username -p password where username is the name of
        the user who wishes to log in and password is the password for that user
*/
int parseArguments(int argc, char *argv[]) {
             	
        // flag indicating the result
        bool passwordRequested = false;

	argumentInit();

	int i;
        for(i = 1; i < argc;)
        {	
		if(argv[i][0] != '-') {
			fputs("Invalid Option\n", stderr);
			return -1;
		}	

		char *option = argv[i];	
		if(strcmp(option, "-p") == 0){
			passwordRequested = true;
			++i;
			continue;
		}

		if(cfuhash_exists(arguments, option)) {
	
			if(cfuhash_get(arguments, option) != NULL){
				fputs("Criteria already set\n", stderr);
                        	return -1;	
			} else if (i == argc - 1) {
				fputs("Invalid Argument Input\n", stderr);
				return -1;
			} else {
				if(isOption(argv[i + 1]) == 0)
					return -1;
				char * argument = calloc(30, (sizeof (char)));	
				strcpy(argument, argv[i + 1]);
				cfuhash_put(arguments, option, argument);
				i += 2;
			}
		} else {
  			fputs("Option not recognised\n", stderr);
                        return -1;
		}
	}

	if(passwordRequested) {
		fputs("Password: ", stdout);
		promptPasswordEntry();	
	}

	// TESTING
	// cfuhash_pretty_print(arguments, stdout);

        return 0;
}

