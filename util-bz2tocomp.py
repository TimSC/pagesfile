
import compressedfile, sys, bz2

if __name__ == "__main__":
	if len(sys.argv) < 2:
		print "Specify input file as argument"
		exit(1)

	if len(sys.argv) < 3:
		print "Specify output file as argument"
		exit(1)

	infi = bz2.BZ2File(sys.argv[1])
	outfiRaw = open(sys.argv[2], "wb")
	outfi = compressedfile.CompressedFile(outfiRaw)

	while True:
		data = infi.read(100000)
		if len(data) == 0: break
		outfi.write(data)
	
	outfi.flush()

