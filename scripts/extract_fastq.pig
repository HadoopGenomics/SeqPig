
A = load '$inputfile' using FastqLoader();
A = LIMIT A $limitval;
STORE A into '$outputfile' using FastqStorer();
