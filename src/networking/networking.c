#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <netdb.h>
#include <netinet/in.h>
#include <string.h>
#include "networking.h"

void readMessage(int sockfd, char *buffer, int size) {
        int n = read(sockfd, buffer, size);
        if (n < 0) {
                perror("ERROR reading from socket");
                exit(1);
        }
	buffer[n] = 0;
}

void sendMessage(int sockfd, char message[], int size) {
        int n = write(sockfd, message, size);
        if (n < 0) {
                perror("ERROR writing to socket");
                exit(1);
        }
}

int openSocket() {

        fputs("Opening Socket\n", stdout);
        /* First call to socket() function */
        int sockfd = socket(AF_INET, SOCK_STREAM, 0);

        if (sockfd < 0) {
                perror("ERROR opening socket");
                exit(1);
        }

	return sockfd;
}

void serverInitializeServerAddress(struct sockaddr_in serv_addr, int portno) {

        fputs("Initializing Socket\n", stdout);
        /* Initialize socket structure */
        bzero(&serv_addr, sizeof(serv_addr));

        serv_addr.sin_family = AF_INET;
        serv_addr.sin_addr.s_addr = INADDR_ANY;
        serv_addr.sin_port = htons(portno);
}

void clientInitializeServerAddress(struct sockaddr_in serv_addr, int portno, const char * host) {

        fputs("Initializing Socket\n", stdout);
        /* Initialize socket structure */
        bzero(&serv_addr, sizeof(serv_addr));

        serv_addr.sin_family = AF_INET;
        serv_addr.sin_port = htons(portno);

	puts("inet_pton\n");
	// now get it back and print it
	char str[255];	
	inet_ntop(AF_INET, &serv_addr.sin_addr, str, INET_ADDRSTRLEN);
	printf("%s:%d\n", str, ntohs(portno)); // prints "192.0.2.33"
	
        if(inet_pton(AF_INET, host, &serv_addr.sin_addr) <= 0)
        {
                printf("\n inet_pton error occured\n");
                exit(1);
        }

	printf("host %s\n", host);

}


void bindSocket(int sockfd, struct sockaddr_in serv_addr) {

        fputs("Binding Socket\n", stdout);
        if (bind(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
                perror("ERROR on binding");
                exit(1);
        }
}

void listenForClient(int sockfd) {

        fputs("Listening\n", stdout);
        listen(sockfd,5);
}

int acceptNewClient(int sockfd, struct sockaddr_in cli_addr) {

        fputs("Accepting\n", stdout);
        int clilen = sizeof(cli_addr);
        
        while (1) {
                int newsockfd = accept(sockfd, (struct sockaddr *) &cli_addr, &clilen);

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

                if (pid == 0) { // child process
                        /* This is the client process */
                        fprintf(stdout, "I am the child process numbered %d\n", pid);
                        fprintf(stdout, "I am communicating with client at %s:%d\n",(char *) inet_ntoa(cli_addr.sin_addr),  cli_addr.sin_port);
                        close(sockfd);
			return newsockfd;
                }
                else { // parent process
                        close(newsockfd);
                }

         } /* end of while */
}


void establishConnection(int sockfd, struct sockaddr_in serv_addr) {
	puts("connect\n");
	
	// now get it back and print it
	char str[255];	
	inet_ntop(AF_INET, &(serv_addr.sin_addr), str, INET_ADDRSTRLEN);
	printf("%s:%d\n", str, ntohs(serv_addr.sin_port)); // prints "192.0.2.33"

	int result = connect(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr));
        if(result < 0){
                printf("\nError Code %d: Connect Failed \n", result);
                exit(1);
        }
}

void outputMessage(char *message) {
	if(fputs(message, stdout) == EOF) {       // this will display succes message sent from the server
        	printf("\nError : fputs error\n");
                exit(1);
        }
	
}


