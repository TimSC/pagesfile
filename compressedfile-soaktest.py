import compressedfile, random, time, pickle, sys
import numpy as np

if __name__=="__main__":
	method = "zlib"
	if len(sys.argv) > 1:
		method = sys.argv[1]

	fi = compressedfile.CompressedFile("soaktest-{0}.dat".format(method), createFile=True, method = method)

	globalRandSeed = 0
	random.seed(globalRandSeed)
	np.random.seed(globalRandSeed)
	numBins = 1000
	
	binSizes = [random.randint(0,500000) for i in range(numBins)]
	print "Total test size", sum(binSizes)
	binOffsets = [0, ]
	for bs in binSizes[:-1]:
		val = binOffsets[-1]+bs
		binOffsets.append(val)

	oldData = None
	oldSeed = None

	while 1:
		print "Begin iteration", globalRandSeed, method
		globalRandSeed += 1
		random.seed(globalRandSeed)
		np.random.seed(globalRandSeed)

		#Generate new data
		data = []
		for bs in binSizes:
			binDat = np.random.bytes(bs)
			data.append(binDat)

		#Generate write order
		accessOrder = range(numBins)[:]
		random.shuffle(accessOrder)

		for count, binNum in enumerate(accessOrder):

			if oldData is not None:
				fi.seek(binOffsets[binNum])
				#readback = fi.read(binSizes[binNum])
				readback = compressedfile.MultiRead(fi, binSizes[binNum])
				checkok = (readback == oldData[binNum])
				if not checkok:
					print "Readback failed. saving to file.", binNum
					print "Matches", sum([x==y for x, y in zip(readback, oldData[binNum])])
					print [ord(c) for c in oldData[binNum][:30]], len(oldData[binNum])
					print [ord(c) for c in readback[:30]], len(readback)
					import pickle
					pickle.dump(oldData[binNum], open("expected.dat","wb"), protocol=-1)
					pickle.dump(readback, open("got.dat","wb"), protocol=-1)
					fi.flush()
					exit(0)
				
			fi.seek(binOffsets[binNum])
			fi.write(data[binNum])

		#Flush every tenth interation to see if it makes a difference
		if globalRandSeed % 10 == 0:
			print "Flushing"
			fi.flush()

		oldData = data
		oldSeed = globalRandSeed

