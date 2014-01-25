import sys, compressedfile

if __name__ == "__main__":
	if len(sys.argv) < 2:
		print "Specify input file as argument"
		exit(1)

	if len(sys.argv) < 3:
		print "Specify output file as argument"
		exit(1)

	infi = compressedfile.CompressedFile(sys.argv[1])
	outfi = open(sys.argv[2], "w+b")

	try:
		os.unlink(sys.argv[2])
	except:
		pass

	while True:
		data = infi.read(100000)
		if len(data) == 0: break
		outfi.write(data)

	outfi.flush()

