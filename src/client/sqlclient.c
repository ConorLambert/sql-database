//#include "sql_client.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "sql_client.h"
// #include <cfuhash.h>

int main (int argc, char *argv[])
{
	if(parseArguments(argc, argv) != -1)
	        startClient();
	else
		fputs("Invalid Argument Input\n", stderr);

        return 0;
}


