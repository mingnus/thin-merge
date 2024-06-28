V=@

THIN_MERGE:=\
	target/release/thin_merge

$(THIN_MERGE):
	$(V) cargo build --release

PREFIX:=/usr
BINDIR:=$(DESTDIR)$(PREFIX)/sbin
DATADIR:=$(DESTDIR)$(PREFIX)/share
MANPATH:=$(DATADIR)/man

STRIP:=strip
INSTALL:=install
INSTALL_DIR = $(INSTALL) -m 755 -d
INSTALL_PROGRAM = $(INSTALL) -m 755
INSTALL_DATA = $(INSTALL) -p -m 644

.SUFFIXES: .txt .8

%.8: %.txt bin/txt2man
	@echo "    [txt2man] $<"
	@mkdir -p $(dir $@)
	$(V) bin/txt2man -t $(basename $(notdir $<)) \
	-s 8 -v "System Manager's Manual" -r "Device Mapper Tools" $< > $@

.PHONY: clean

clean:
	cargo clean
	$(RM) man8/*.8

MANPAGES:=man8/thin_merge.8

install: $(THIN_MERGE) $(MANPAGES)
	$(INSTALL_DIR) $(BINDIR)
	$(INSTALL_PROGRAM) $(THIN_MERGE) $(BINDIR)
	$(STRIP) $(BINDIR)/thin_merge
	$(INSTALL_DIR) $(MANPATH)/man8
	$(INSTALL_DATA) man8/thin_merge.8 $(MANPATH)/man8

.PHONY: install
