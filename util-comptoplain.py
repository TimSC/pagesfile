import sys, compressedfile

if __name__ == "__main__":
	if len(sys.argv) < 2:
		print "Specify input file as argument"
		exit(1)

	if len(sys.argv) < 3:
		print "Specify output file as argument"
		exit(1)

	infiRaw = open(sys.argv[1], "rb")
	infi = compressedfile.CompressedFile(infiRaw)
	outfi = open(sys.argv[2], "wb")

	print len(infi)

	while True:
		data = infi.read(100000)
		if len(data) == 0: break
		outfi.write(data)

	outfi.flush()

