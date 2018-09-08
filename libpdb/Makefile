ROOT=..
include $(ROOT)/buildarch/preamble.mk

LIBDIR=..

PDBC=	pdb-bins.c			\
	pdb-bins-strtable.c		\
	pdb-bins-numtable.c		\
	pdb-checkpoint.c		\
	pdb-concentric.c		\
	pdb-configure.c			\
	pdb-count.c			\
	pdb-create.c			\
	pdb-database-id.c		\
	pdb-database-path.c		\
	pdb-destroy.c			\
	pdb-disk.c			\
	pdb-generation.c		\
	pdb-hash.c			\
	pdb-id.c			\
	pdb-index.c			\
	pdb-index-bmap.c		\
	pdb-index-hmap.c		\
	pdb-index-gmap.c		\
	pdb-initialize.c		\
	pdb-is-remote-mounted.c		\
	pdb-iterator.c			\
	pdb-iterator-all.c		\
	pdb-iterator-bgmap.c		\
	pdb-iterator-gmap.c		\
	pdb-iterator-hmap.c		\
	pdb-iterator-null.c		\
	pdb-iterator-suspend.c		\
	pdb-iterator-util.c		\
	pdb-iterator-by-name.c		\
	pdb-iterator-smap.c		\
	pdb-linkage.c			\
	pdb-local-ip.c			\
	pdb-lockfile.c			\
	pdb-log.c			\
	pdb-mem.c			\
	pdb-msclock.c			\
	pdb-prefix.c			\
	pdb-primitive-alloc.c		\
	pdb-primitive-alloc-subscription.c	\
	pdb-primitive-compress.c	\
	pdb-primitive-dump.c		\
	pdb-primitive-finish.c		\
	pdb-primitive-initialize.c	\
	pdb-primitive-n.c		\
	pdb-primitive-read.c		\
	pdb-primitive-reference.c	\
	pdb-primitive-previous.c	\
	pdb-primitive-summary.c		\
	pdb-restore.c			\
	pdb-refresh.c			\
	pdb-runtime-statistics.c	\
	pdb-rxs.c	\
	pdb-set-database-id.c		\
	pdb-set-database-name.c		\
	pdb-set-mode.c			\
	pdb-set-path.c			\
	pdb-set-predictable.c		\
	pdb-set-sync.c			\
	pdb-strerror.c			\
	pdb-sync.c			\
	pdb-status.c			\
	pdb-truncate.c			\
	pdb-util.c			\
	pdb-verify.c			\
	pdb-versioned.c			\
	pdb-vip.c			\
	pdb-word.c
PDBO=	$(PDBC:%.c=$(OBJ)/%.o)
PDBH=	pdb.h pdbp.h pdb-primitive.h pdb-bins-numtable.h

INCOPTS=-I$(LIBDIR)/libgraph/		\
	-I$(LIBDIR)/libaddb/		\
	-I$(LIBDIR)/libcl/		\
	-I$(LIBDIR)/libcm

CFLAGS+=$(INCOPTS)

LIBOPTS=-L$(LIBDIR)/libgraph/$(OBJ)	\
	-L$(LIBDIR)/libaddb/$(OBJ)	\
	-L$(LIBDIR)/libcl/$(OBJ)	\
	-L$(LIBDIR)/libcm/$(OBJ)

LIBLIBS= -lgraph -laddb -lcl -lcm $(OSLIBS)

LIBS=	$(LIBDIR)/libgraph/$(OBJ)/libgraph.a	\
	$(LIBDIR)/libaddb/$(OBJ)/libaddb.a	\
	$(LIBDIR)/libcl/$(OBJ)/libcl.a		\
	$(LIBDIR)/libcm/$(OBJ)/libcm.a

INCH=	$(LIBDIR)/libgraph/graph.h	\
	$(LIBDIR)/libaddb/addb.h	\
	$(LIBDIR)/libcl/cl.h		\
	$(LIBDIR)/libcm/cm.h		\
	$(LIBDIR)/libaddb/addb-bmap.h	\

.PHONY:	all clean distclean subdirs

all:	tags 			\
	subdirs			\
	$(OBJ)/libpdb.a		\
	$(OBJ)/pdbdump		\
	$(OBJ)/pdbname

subdirs:
	@-test -d $(OBJ) || mkdir $(OBJ)

$(OBJ)/%.o: %.c $(PDBH) $(INCH)
	$(COMPILE.c) -o $@ $<

$(LIBDIR)/libgraph/$(OBJ)/libgraph.a:
	@$(MAKE) --no-print-directory -C $(LIBDIR)/libgraph

$(LIBDIR)/libaddb/$(OBJ)/libaddb.a:
	@$(MAKE) --no-print-directory -C $(LIBDIR)/libaddb

$(LIBDIR)/libcl/$(OBJ)/libcl.a:
	@$(MAKE) --no-print-directory -C $(LIBDIR)/libcl

$(LIBDIR)/libcm/$(OBJ)/libcm.a:
	@$(MAKE) --no-print-directory -C $(LIBDIR)/libcm

tags:	$(PDBC) $(PDBH)
	$(CTAGS) $(PDBC) $(PDBH)

$(OBJ)/libpdb.a:	$(PDBO)
	$(BUILDVERSION) pdb $(PDBC) $(PDBH) > $(OBJ)/pdb-build-version.c
	$(COMPILE.c) -c $(OBJ)/pdb-build-version.c 	\
		     -o $(OBJ)/pdb-build-version.o
	@-rm -f $@
	$(AR) cr $@ $(PDBO) $(OBJ)/pdb-build-version.o
	$(RANLIB) $@
	@-rm $(OBJ)/pdb-build-version.*

$(OBJ)/pdbname.o: pdb-set-database-name.c
	$(COMPILE.c) -DUTILITY_PDBNAME -o $@ $<

$(OBJ)/pdbname:	$(OBJ)/pdbname.o
	$(CC) $(CFLAGS) -o $(OBJ)/pdbname $(OBJ)/pdbname.o $(OSLIBS)

$(OBJ)/pdbdump.o: pdbdump.c $(INCH) $(PDBH)
	$(COMPILE.c) -o $@ $<

$(OBJ)/pdbdump:	$(OBJ)/pdbdump.o $(LIBS) $(PDBH)
	$(CC) $(CFLAGS) -o $(OBJ)/pdbdump $(LIBOPTS) $(OBJ)/pdbdump.o 	\
		./$(OBJ)/libpdb.a $(LIBLIBS)

clean:
	rm -f $(PDBO)

distclean:
	rm -rf obj.* doc

docs:
	@-:
	# one day...

test:
	@-:
	# one day...

obj-export:
	@echo export OBJ=$(OBJ)
