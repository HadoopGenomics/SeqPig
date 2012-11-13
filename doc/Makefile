
LATEX_SOURCES = seqpig_examples.tex seqpig_installation.tex seqpig_manual.tex
HTML_DOCS = seqpig_manual.html
PDF_DOCS = seqpig_manual.pdf
TOC_DOCS = seqpig_manual.toc
TMP_FILES = seqpig_manual.log seqpig_manual.css seqpig_manual.tmp seqpig_manual.dvi seqpig_manual.idv seqpig_manual.lg seqpig_manual.aux seqpig_manual.4tc seqpig_manual.4ct seqpig_manual.xref seqpig_manual.out

all: html pdf

pdf: $(PDF_DOCS)

html: $(HTML_DOCS)

clean:
	rm -f $(HTML_DOCS) $(PDF_DOCS) $(TOC_DOCS) $(TMP_FILES)

%.pdf : %.tex %.toc $(LATEX_SOURCES)
	pdflatex $<
	pdflatex $<

%.html : %.tex %.toc $(LATEX_SOURCES)
	htlatex $< "xhtml"

%.toc : %.tex
	pdflatex $<
	pdflatex $<
