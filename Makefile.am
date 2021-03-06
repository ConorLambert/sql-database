## Process this file with automake to produce Makefile.in

##@VALGRIND_CHECK_RULES@
#VALGRIND_SUPPRESSIONS_FILES = my-project.supp
#EXTRA_DIST = my-project.supp
#ACLOCAL_AMFLAGS = -I m4


lib_LTLIBRARIES = libclient.la libserver.la libdiskio.la libaccess.la libparser.la libbtree.la libcheck.la libutility.la
libutility_la_SOURCES = libs/libutility/utility.c libs/libutility/utility.h
libdiskio_la_SOURCES = src/server/storage/diskio.c src/server/storage/diskio.h
libbtree_la_SOURCES = libs/libbtree/btree.c libs/libbtree/btree.h
libdiskio_la_LIBADD = libbtree.la libutility.la
libaccess_la_SOURCES = src/server/access/sqlaccess.c src/server/access/sqlaccess.h
libaccess_la_LIBADD = libdiskio.la libs/libcfu/src/libcfu.la libutility.la
libparser_la_SOURCES = src/server/access/sql_parser.c src/server/access/sql_parser.h
libparser_la_LIBADD = libaccess.la libutility.la
libclient_la_SOURCES = src/client/sql_client.c src/client/sql_client.h
libclient_la_LIBADD = libs/libcfu/src/libcfu.la libutility.la
libserver_la_SOURCES = src/server/sql_server.c src/server/sql_server.h
libserver_la_LIBADD = libaccess.la libutility.la 
libcheck_la_SOURCES = tests/check_utility.c tests/check_utility.h
libcheck_la_LIBADD = libaccess.la libutility.la



bin_PROGRAMS = sqlclient sqlserver
sqlclient_SOURCES = src/client/sqlclient.c
sqlclient_LDADD = libclient.la
sqlserver_SOURCES = src/server/sqlserver.c
sqlserver_LDADD = libserver.la libdiskio.la libs/libcfu/src/libcfu.la


TESTS = check_access check_sql_client check_commit check_storage check_sql_parser 
check_PROGRAMS = check_access check_sql_client check_commit check_storage check_sql_parser 
check_sql_client_SOURCES = tests/check_sql_client.c src/client/sql_client.h
check_sql_client_CFLAGS = ${check_CFLAGS}
check_sql_client_LDADD = ${check_LIBS} $(top_builddir)/libclient.la $(top_builddir)/libutility.la @CHECK_LIBS@
check_storage_SOURCES = tests/check_storage.c src/server/storage/diskio.h
check_storage_CFLAGS = ${check_CFLAGS}
check_storage_LDADD = ${check_LIBS} $(top_builddir)/libdiskio.la $(top_builddir)/libbtree.la $(top_builddir)/libutility.la @CHECK_LIBS@
check_access_SOURCES = tests/check_access.c tests/check_utility.h src/server/access/sqlaccess.h src/server/storage/diskio.h libs/libbtree/btree.h libs/libcfu/src/cfuhash.h
check_access_CFLAGS = ${check_CFLAGS}
check_access_LDADD = ${check_LIBS} $(top_builddir)/libaccess.la $(top_builddir)/libbtree.la $(top_builddir)/libdiskio.la $(top_builddir)/libs/libcfu/src/libcfu.la $(top_builddir)/libcheck.la $(top_builddir)/libutility.la @CHECK_LIBS@
check_commit_SOURCES = tests/check_commit.c src/server/access/sqlaccess.h src/server/storage/diskio.h libs/libbtree/btree.h libs/libcfu/src/cfuhash.h
check_commit_CFLAGS = ${check_CFLAGS}
check_commit_LDADD = ${check_LIBS} $(top_builddir)/libaccess.la $(top_builddir)/libbtree.la $(top_builddir)/libdiskio.la $(top_builddir)/libs/libcfu/src/libcfu.la $(top_builddir)/libcheck.la $(top_builddir)/libutility.la @CHECK_LIBS@
check_sql_parser_SOURCES = tests/check_sql_parser.c src/server/access/sql_parser.h libs/libutility/utility.h
check_sql_parser_CFLAGS = ${check_CFLAGS}
check_sql_parser_LDADD = ${check_LIBS} $(top_builddir)/libaccess.la $(top_builddir)/libbtree.la $(top_builddir)/libs/libcfu/src/libcfu.la $(top_builddir)/libparser.la $(top_builddir)/libdiskio.la $(top_builddir)/libutility.la


#.PHONY: check-valgrind ;
#check-valgrind: $(TESTS)
#    @for test in $$(echo $(TESTS) | sed 's/tests\//tests\/.libs\//g') ; do \
#        CK_FORK=no LD_LIBRARY_PATH=h264bitstream/.libs $(VALGRIND) --error-exitcode=1 --leak-resolution=low --quiet \
#            --leak-check=full --show-possibly-lost=no --suppressions=tests/valgrind.suppressions $${test} ; \
#    done
