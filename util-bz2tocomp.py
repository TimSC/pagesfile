
import compressedfile, sys, bz2, os

if __name__ == "__main__":
	if len(sys.argv) < 2:
		print "Specify input file as argument"
		exit(1)

	if len(sys.argv) < 3:
		print "Specify output file as argument"
		exit(1)

	try:
		os.unlink(sys.argv[2])
	except:
		pass

	infi = bz2.BZ2File(sys.argv[1])
	outfi = compressedfile.CompressedFile(sys.argv[2])

	while True:
		data = infi.read(1000000)
		if len(data) == 0: break
		outfi.write(data)

		pos = outfi.tell()
		print pos
	
	outfi.flush()

