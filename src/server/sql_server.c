#include <stdio.h>
#include <stdlib.h>
#include <netdb.h>
#include <netinet/in.h>
#include <string.h>

int sockfd, newsockfd, portno, clilen;
char buffer[256];
struct sockaddr_in serv_addr, cli_addr;
int portno = 5001;

/*
	parse request to find out what the request wants
	return integer indicating the success of the parse 
*/
int parseRequest(char request[]) {
	// find out what the request is and act accordingly
	return 0;	
}

void acceptRequest(char buffer[]) {
	int n = read(newsockfd, buffer, 255);   
	if (n < 0) {
		perror("ERROR reading from socket");
		exit(1);
	}

	// parse request and store any response in buffer
	if(parseRequest(buffer) == -1){
		perror("ERROR processing request");
		exit(1);
	}
}

/*
	send response back to user and return the number of characters written
*/
void sendResponse(char response[]) {
	int n = write(newsockfd, response, 256);
	if (n < 0) {
		perror("ERROR writing to socket");
		exit(1);
	}	
}


void processRequests() {
	char buffer[256];
   	bzero(buffer, 256);
	
	fputs("Processing Requests\n", stdout);
	while(1) {
		acceptRequest(buffer);
		sendResponse(buffer);
	}
}


void openSocket() {
	
	fputs("Opening Socket\n", stdout);
	/* First call to socket() function */
   	sockfd = socket(AF_INET, SOCK_STREAM, 0);
   
   	if (sockfd < 0) {
      		perror("ERROR opening socket");
      		exit(1);
   	}		
}	

void initializeSocket() {

	fputs("Initializing Socket\n", stdout);
	/* Initialize socket structure */
   	bzero((char *) &serv_addr, sizeof(serv_addr));
   
   	serv_addr.sin_family = AF_INET;
   	serv_addr.sin_addr.s_addr = INADDR_ANY;
   	serv_addr.sin_port = htons(portno);
}

void bindSocket() {

	fputs("Binding Socket\n", stdout);
	if (bind(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
      		perror("ERROR on binding");
      		exit(1);
   	}	
}

void _listen() {

	fputs("Listening\n", stdout);
	listen(sockfd,5);
}

void _accept() {

	fputs("Accepting\n", stdout);
	clilen = sizeof(cli_addr);
   
   	while (1) {
      		newsockfd = accept(sockfd, (struct sockaddr *) &cli_addr, &clilen);
		
      		if (newsockfd < 0) {
         		perror("ERROR on accept");
         		exit(1);
      		}
      
      		/* Create child process */
		fputs("Forking\n", stdout);
      		int pid = fork();
		
      		if (pid < 0) {
         		perror("ERROR on fork");
         		exit(1);
      		}
      
      		if (pid == 0) {
         		/* This is the client process */
			fputs("I am the child\n", stdout);
         		close(sockfd);
         		processRequests();
         		exit(0);
      		}
      		else {
         		close(newsockfd);
      		}
		
	 } /* end of while */
}

void startup() {
	openSocket();

	initializeSocket();
	
	bindSocket();
	
	_listen();

	_accept();
}
