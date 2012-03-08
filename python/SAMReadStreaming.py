#/usr/bin/python

def coverageFunc(index, reads):
   return len(reads)

def SNPFunc(index, reads):
   return len(reads)

funcdict = {
  'coverageFunc': coverageFunc,
  'SNPFunc': SNPFunc
}

@outputSchema("y:bag{t:tuple(position:int, counter:int)}")
def readCoverage(start_index, end_index, function_name, bag):
   def filterFuncSmall(read, thresh):
      if read != None and tuple(read)[2] <= thresh:
         return read

   def filterFuncLarge(read, thresh):
      if read != None and tuple(read)[2] > thresh:
         return read

   outBag = []
   reads = []
   prev_index = -1
   counter = 0

   for samrec in bag:
        # note: we need to shop of the last value
        #samrec = samrec[:len(samrec) - 1]
        start_this_read = samrec[1]
	
	if prev_index == -1:
           if start_this_read > start_index:
              prev_index = start_this_read
           else:
              prev_index = start_index

        if len(reads) > 0:
           # first, get rid of all reads that end before this one starts (there cannot be
           # any more reads starting before this one since they arrive in the order of
           # start coordindate by assumption)
           while len(reads) > 0 and reads[0][2] <  start_this_read:
	      end_cur_read = reads[0][2]

	      for i in range(prev_index, end_cur_read+1):
	         tup=(i, funcdict[function_name](i, reads))
     	         outBag.append(tup)
              reads = reads[1:]
	      prev_index = end_cur_read + 1

           # note: the number of "active" reads has not changed since prev_index and start_this_read,
           # so we produce the same output for this range of coordinates
           for i in range(prev_index, start_this_read):
              tup=(i, funcdict[function_name](i, reads))
              outBag.append(tup)

           # now place this read in list at a position according to its end coordinate
           # if it is not alreay in the list (maybe both start and end coordinate lie in
           # our bucket)
           # NOTE: we now assume that ther are NO DUPLICATES (i.e., DISTINCT has been called)
           #if samrec not in reads:
           #if not any(r for r in reads if r[0] == samrec[0] and r[1] == samrec[1] and r[2] == samrec[2]):
           threshl = [samrec[2]]*len(reads)   
           smaller_reads = map(filterFuncSmall, reads, threshl)
           larger_reads = map(filterFuncLarge, reads, threshl)
        
           smaller_reads.append(samrec)
           smaller_reads.extend(larger_reads)
           reads = filter(None, smaller_reads)
        else:
           reads.append(samrec)

        counter = counter + 1

        if start_this_read > prev_index:
           prev_index = start_this_read

        # if this is the last read of the bucket, clear the reads list in order of end
        # coordinate and produce output
        if counter == len(bag):
           while len(reads) > 0:
	      end_cur_read = reads[0][2]

              if end_cur_read >= end_index:
                 end_cur_read = end_index - 1

	      for i in range(prev_index, end_cur_read+1):
	         tup=(i, funcdict[function_name](i, reads))
     	         outBag.append(tup)

              reads = reads[1:]
	      prev_index = end_cur_read + 1
              if prev_index >= end_index:
                 break

   return outBag 

#@outputSchema("y:bag{t:tuple(name:chararray, start:int, counter:int)}") 
#def collectBag(bag):
#   outBag = []
#   counter = 0
#   for samrec in bag:
#     tup=(samrec[0], samrec[1], counter)
#     outBag.append(tup)
#     counter = counter + 1
#   return outBag
