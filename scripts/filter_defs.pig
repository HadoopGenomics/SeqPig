-- interpretations of flags based on http://picard.sourceforge.net/explain-flags.html
DEFINE ReadPaired fi.aalto.seqpig.filter.SAMFlagsFilter('HasMultipleSegments');
DEFINE ReadMappedInPair fi.aalto.seqpig.filter.SAMFlagsFilter('IsProperlyAligned');
DEFINE ReadUnmapped fi.aalto.seqpig.filter.SAMFlagsFilter('HasSegmentUnmapped');
DEFINE MateUnmapped fi.aalto.seqpig.filter.SAMFlagsFilter('NextSegmentUnmapped');
DEFINE ReadReverseStrand fi.aalto.seqpig.filter.SAMFlagsFilter('IsReverseComplemented');
DEFINE MateReverseStrand fi.aalto.seqpig.filter.SAMFlagsFilter('NextSegmentReversed');
DEFINE FirstInPair fi.aalto.seqpig.filter.SAMFlagsFilter('IsFirstSegment');
DEFINE SecondInPair fi.aalto.seqpig.filter.SAMFlagsFilter('IsLastSegment');
DEFINE NotPrimaryAlignment fi.aalto.seqpig.filter.SAMFlagsFilter('HasSecondaryAlignment');
DEFINE ReadFailsQC fi.aalto.seqpig.filter.SAMFlagsFilter('FailsQC');
DEFINE IsDuplicate fi.aalto.seqpig.filter.SAMFlagsFilter('IsDuplicate');