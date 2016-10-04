# Process this file with autoconf to produce a configure script.

# Prelude.
AC_PREREQ([2.59])
AC_INIT([SQL-DATABASE], [0.1])
AC_CONFIG_MACRO_DIR([m4])

# place to put some extra build scripts installed
AC_CONFIG_AUX_DIR([build-aux])

# check for programs
LT_INIT

# unique source file --- primitive safety check 
AC_CONFIG_SRCDIR([src/client/sqlclient.c])

# fairly severe build strictness
# change foreign to gnu or gnits to comply with gnu standards
AM_INIT_AUTOMAKE([-Wall -Werror foreign 1.9.6], [subdir-objects])

# Checks for programs.
AC_PROG_CC
AC_PROG_LIBTOOL
AM_PROG_CC_C_O

AX_VALGRIND_DFLT([sgcheck], [off])
AX_VALGRIND_CHECK

# Checks for libraries.
PKG_CHECK_MODULES([check], [check])

# AC_SUBST([GLib_CFLAGS], [pkg-config --cflags glib-2.0])
# AC_SUBST(GLib_LIBS)

# This macro is defined in check.m4 and tests if check.h and
# libcheck.a are installed in your system. It sets CHECK_CFLAGS and
# CHECK_LIBS accordingly.  
#  AM_PATH_CHECK([MINIMUM-VERSION,
#                [ACTION-IF-FOUND [, ACTION-IF-NOT-FOUND]]])
AM_PATH_CHECK()

# Checks for header files.
AC_HEADER_STDC
AC_CHECK_HEADERS([stdlib.h])

# Checks for typedefs, structures, and compiler characteristics.

# Checks for library functions.
AC_FUNC_MALLOC

# Output files
AC_CONFIG_HEADERS([config.h])

AC_CONFIG_FILES([Makefile])

AC_OUTPUT
