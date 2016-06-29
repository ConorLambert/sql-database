/*
        #include "Client.h"

        #include "Networking.h"

        #include "SQL_Module.h"
        */


#include <stdlib.h>
#include <stdio.h>
#include <string.h>

void changePassword(void) {}

void parseQuery(char* query) {}

void submitQuery(char* query) {}

char* password;

char* username;

char *host;

/*
FILE * openConfigFile(void) {return NULL;}

void closeConfigFile(FILE * config_file) {}

void getPassword(void) {}

void getUsername(void) {}

void establishConnection(void) {}
*/

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


int isOption(char *argument) {
	
	if(strcmp(argument, "-u") == 0 || strcmp(argument, "-p") == 0 || strcmp(argument, "-h") == 0) {
		return -1;
	}
	
	return 0;
}

/*
        Input arguments have a pattern of -option argument -option argument
        For example -u username -p password where username is the name of
        the user who wishes to log in and password is the password for that user
*/
int parseArguments(int argc, char *argv[]) {

        // flags indicating necessary data inputted
        int haveUsername = 0;
        int havePassword = 0;
        int haveHost = 0;

        // flag indicating the result
        int result = 0;

        int i;
        for(i = 1; i < argc; i += 2)
        {
                // get option
                // if option is username (-u)
                if(strcmp(argv[i], "-u") == 0) {
                        if(haveUsername) {
                                fputs("Cannot have two usernames\n", stderr);
                                return -1;
                        }
			
			if((i == argc - 1) || isOption(argv[i + 1])) {
                                fputs("No Username defined\n", stderr);
                                return -1;
                        }

                        username = argv[i + 1];
                        haveUsername = 1;
                        result = 1;
                }

		else if(strcmp(argv[i], "-h") == 0) {
                        if(haveHost) {
                                fputs("Cannot have two hosts\n", stderr);
                                return -1;
                        }

			if(i == argc - 1 || isOption(argv[i + 1])) {
				fputs("No host defined\n", stderr);
				return -1;
			}

                        host = argv[i + 1];
                        haveHost = 1;
                        result = 1;
                }

                // else if option is password (-p)
                else if(strcmp(argv[i], "-p") == 0) {

                        if(havePassword) {
                                fputs("Cannot have two passwords\n", stderr);
                                return -1;
                        }

                        havePassword = 1;
                        result = 1;
                }

                else {
                        fputs("Option not recognised\n", stderr);
			printf("i = %d, argv[i] = %s, argv[i + 1] = %s\n", i, argv[i], argv[i + 1]);
                        return -1;
                }

	}


        // check what parameters where not passed
        if(!haveUsername)
                puts("No username entered\n");  // search config file
        else
                printf("Username entered is %s\n", username);
        if(!haveHost)
                puts("No host entered\n");      // search config file
        else
                printf("Host entered is %s\n", host);
        if(!havePassword)
                puts("No password entered\n");  // search config file
        else
                printf("Trigger password entry\n"); // triggerPasswordEntry();


        return 1;
}
