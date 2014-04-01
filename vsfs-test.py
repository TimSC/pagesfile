
import vsfs, os

if __name__=="__main__":
	fs = vsfs.Vsfs("test.vsfs", 1)
	fs._print_layout()
	#fs._create_inode(0, 1)
	for i in range(30):
		fs._create_file("test{0}".format(i), 7000, 0)
	print fs.listdir("/")

	fi =  fs.open("test1","w")
	print fi
	print len(fi)
	testStr = "".join([chr(i % 256) for i in range(6000)])

	fi.write(testStr)
	fi.close()

	fi2 =  fs.open("test1","r")
	testStr2 = fi2.read(8000)
	count = 0
	for a, b in zip(testStr, testStr2):
		count += 1
	print "correct", count
	
