
all: seqpig_installation.html seqpig_examples.html seqpig_installation.pdf seqpig_examples.pdf

clean:
	rm -f ${DOCS}.{pdf,html,aux,log}

%.pdf : %.tex
	pdflatex $<

%.html : %.tex
	htlatex $< "xhtml"

