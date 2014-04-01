
import vsfs, os

def CreateMultipleFilesHacky():
	fs = vsfs.Vsfs("test.vsfs", 1)
	#fs._create_inode(0, 1)
	for i in range(30):
		fs._create_file("test{0}".format(i), 7000, 0)
	found = fs.listdir("/")
	if len(found) != 30:
		os.unlink("test.vsfs")
		raise Exception("Wrong number of files")
	for i in range(30):
		if 	"test{0}".format(i) not in found:
			os.unlink("test.vsfs")
			raise Exception("Missing file")
	os.unlink("test.vsfs")
	return 1

def ReadAndWriteHacky():
	fs = vsfs.Vsfs("test.vsfs", 1)
	#fs._create_inode(0, 1)
	#for i in range(30):
	#	fs._create_file("test{0}".format(i), 7000, 0)

	fi =  fs.open("test1","w")
	if len(fi) != 0:
		raise Exception("Wrong file length")
	
	testStr = "".join([chr(i % 256) for i in range(6000)])

	fi.write(testStr)
	fi.close()

	fi2 =  fs.open("test1","r")
	testStr2 = fi2.read(8000)
	count = 0
	for a, b in zip(testStr, testStr2):
		count += 1
	if count != 6000:
		raise Exception("Read back failed")
	
	del fi
	del fi2
	del fs

	fs = vsfs.Vsfs("test.vsfs")
	#fs._print_layout()

	fi2 =  fs.open("test1","r")
	testStr2 = fi2.read(8000)
	count = 0
	for a, b in zip(testStr, testStr2):
		count += 1
	if count != 6000:
		raise Exception("Read back failed (2)")

	return 1

if __name__=="__main__":
	print "CreateMultipleFiles test", CreateMultipleFilesHacky()
	print "ReadAndWrite test", ReadAndWriteHacky()

