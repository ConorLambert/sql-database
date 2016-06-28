## Process this file with automake to produce Makefile.in

lib_LTLIBRARIES = libclient.la
libclient_la_SOURCES = sql_client.c sql_client.h

bin_PROGRAMS = main
main_SOURCES = main.c
main_LDADD = libclient.la
