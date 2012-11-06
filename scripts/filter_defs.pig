-- interpretations of flags based on http://picard.sourceforge.net/explain-flags.html
DEFINE ReadPaired fi.aalto.seqpig.SAMFlagsFilter('HasMultipleSegments');
DEFINE ReadMappedInPair fi.aalto.seqpig.SAMFlagsFilter('IsProperlyAligned');
DEFINE ReadUnmapped fi.aalto.seqpig.SAMFlagsFilter('HasSegmentUnmapped');
DEFINE MateUnmapped fi.aalto.seqpig.SAMFlagsFilter('NextSegmentUnmapped');
DEFINE ReadReverseStrand fi.aalto.seqpig.SAMFlagsFilter('IsReverseComplemented');
DEFINE MateReverseStrand fi.aalto.seqpig.SAMFlagsFilter('NextSegmentReversed');
DEFINE FirstInPair fi.aalto.seqpig.SAMFlagsFilter('IsFirstSegment');
DEFINE SecondInPair fi.aalto.seqpig.SAMFlagsFilter('IsLastSegment');
DEFINE NotPrimaryAlignment fi.aalto.seqpig.SAMFlagsFilter('HasSecondaryAlignment');
DEFINE ReadFailsQC fi.aalto.seqpig.SAMFlagsFilter('FailsQC');
DEFINE IsDuplicate fi.aalto.seqpig.SAMFlagsFilter('IsDuplicate');