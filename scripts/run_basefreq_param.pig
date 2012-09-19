A = load '$inputfile' using BamUDFLoader('yes');
A = FOREACH A GENERATE read, flags, refname, start, cigar, mapqual;
A = FILTER A BY (flags/4)%2==0;
RefPos = FOREACH A GENERATE ReadRefPositions(read, flags, refname, start, cigar), mapqual;
flatset = FOREACH RefPos GENERATE flatten($0), mapqual;
grouped = GROUP flatset BY ($0, $1, $2) PARALLEL $pparallel;
base_counts = FOREACH grouped GENERATE group.chr, group.pos, group.base, COUNT(flatset);
base_counts = ORDER base_counts BY chr,pos PARALLEL $pparallel;
store base_counts into '$outputfile';
