
HTML_DOCS = seqpig_manual.html
PDF_DOCS = seqpig_manual.pdf
TOC_DOCS = seqpig_manual.toc
TMP_FILES = seqpig_manual.log seqpig_manual.css seqpig_manual.tmp seqpig_manual.dvi seqpig_manual.idv seqpig_manual.lg seqpig_manual.aux seqpig_manual.4tc seqpig_manual.4ct seqpig_manual.xref

all: $(HTML_DOCS) $(PDF_DOCS)

clean:
	rm -f $(HTML_DOCS) $(PDF_DOCS) $(TOC_DOCS) $(TMP_FILES)

%.pdf : %.tex %.toc
	pdflatex $<

%.html : %.tex %.toc
	htlatex $< "xhtml"

%.toc : %.tex
	latex $<
	latex $<
