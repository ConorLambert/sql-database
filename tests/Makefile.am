TESTS = check_sql_client
check_PROGRAMS = check_sql_client
check_sql_client_SOURCES = check_sql_client.c $(top_builddir)/src/sql_client.h
check_sql_client_LDADD = $(top_builddir)/src/libclient.la @CHECK_LIBS@

@VALGRIND_CHECK_RULES@
VALGRIND_SUPPRESSIONS_FILES = my-project.supp
EXTRA_DIST = my-project.supp

