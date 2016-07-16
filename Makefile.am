## Process this file with automake to produce Makefile.in

lib_LTLIBRARIES = libclient.la libserver.la libdiskio.la libaccess.la
libdiskio_la_SOURCES = src/server/storage/diskio.c src/server/storage/diskio.h
libaccess_la_SOURCES = src/server/access/sqlaccess.c src/server/access/sqlaccess.h
libaccess_la_LIBADD = libdiskio.la
libclient_la_SOURCES = src/client/sql_client.c src/client/sql_client.h
libclient_la_LIBADD = libs/libcfu/src/libcfu.la
libserver_la_SOURCES = src/server/sql_server.c src/server/sql_server.h
libserver_la_LIBADD = libaccess.la


bin_PROGRAMS = sqlclient sqlserver
sqlclient_SOURCES = src/client/sqlclient.c
sqlclient_LDADD = libclient.la
sqlserver_SOURCES = src/server/sqlserver.c
sqlserver_LDADD = libserver.la

TESTS = check_sql_client
check_PROGRAMS = check_sql_client
check_sql_client_SOURCES = tests/check_sql_client.c src/client/sql_client.h
check_sql_client_LDADD = $(top_builddir)/libclient.la @CHECK_LIBS@

