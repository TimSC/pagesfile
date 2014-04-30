import compressedfile, random, time, pickle
import numpy as np

if __name__=="__main__":
	fi = compressedfile.CompressedFile("soaktest.dat", createFile=True)

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
		print "Begin interation", globalRandSeed
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
				readback = fi.read(binSizes[binNum])
				checkok = (readback == oldData[binNum])
				if not checkok:
					print "Readback failed. saving to file.", binNum
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

