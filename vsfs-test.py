
import vsfs, os

def CreateMultipleFiles():
	fs = vsfs.Vsfs("test.vsfs", 1)
	for i in range(30):
		fi = fs.open("test{0}".format(i),"w")
	found = fs.listdir("/")
	if len(found) != 30:
		os.unlink("test.vsfs")
		raise Exception("Wrong number of files")
	for i in range(30):
		if "test{0}".format(i) not in found:
			os.unlink("test.vsfs")
			raise Exception("Missing file")
	os.unlink("test.vsfs")
	return 1

def ReadAndWrite():
	fs = vsfs.Vsfs("test.vsfs", 1)

	fi =  fs.open("test1","w")
	if len(fi) != 0:
		raise Exception("Wrong file length")
	
	testStr = "".join([chr(i % 256) for i in range(6000)])

	fi.write(testStr)
	if len(fi) != 6000:
		raise Exception("Wrong file length")

	fi.close()

	fi2 =  fs.open("test1","r")
	if len(fi2) != 6000:
		raise Exception("Wrong file length")

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

def FileHandleSync():
	fs = vsfs.Vsfs("test.vsfs", 1)

	fi =  fs.open("test1","w")
	if len(fi) != 0:
		raise Exception("Wrong file length")
	
	fi2 =  fs.open("test1","r")
	if len(fi2) != 0:
		raise Exception("Wrong file length")

	testStr = "".join([chr(i % 256) for i in range(6000)])
	fi.write(testStr)

	if len(fi) != 6000:
		raise Exception("Wrong file length")

	if len(fi2) != 6000:
		raise Exception("Wrong file length")

	return 1

def CreateAndDeleteFile():
	fs = vsfs.Vsfs("test.vsfs", 1)

	fi =  fs.open("test1","w")
	if len(fi) != 0:
		raise Exception("Wrong file length")

	fi.write("foobar")

	fi.close()
	del fi
	if len(fs.listdir("/")) != 1:
		raise Exception("Wrong number of files")

	fs.rm("test1")

	if len(fs.listdir("/")) != 0:
		raise Exception("Wrong number of files")
	return 1

def CreateUseAndDeleteFolder():
	fs = vsfs.Vsfs("test.vsfs", 1)
	fs.mkdir("/foo", 0)
	if len(fs.listdir("/")) != 1:
		raise Exception("Failed to create folder")

	fi = fs.open("/foo/test.txt", "w")
	fi.write("foobar")
	fi.close()

	fs.rm("/foo/test.txt")

	fs.rmdir("/foo")
	if len(fs.listdir("/")) != 0:
		raise Exception("Failed to delete folder")
	return 1

def CreateUseAndDeleteNestedFolder():
	fs = vsfs.Vsfs("test.vsfs", 1)
	fs.mkdir("/foo", 0)
	if len(fs.listdir("/")) != 1:
		raise Exception("Failed to create folder")

	fs.mkdir("/foo/bar", 0)
	if len(fs.listdir("/foo")) != 1:
		raise Exception("Failed to create folder")

	fi = fs.open("/foo/bar/test.txt", "w")
	fi.write("foobar")
	fi.close()

	fs.rm("/foo/bar/test.txt")

	fs.rmdir("/foo/bar")
	if len(fs.listdir("/foo")) != 0:
		raise Exception("Failed to delete folder")

	fs.rmdir("/foo")
	if len(fs.listdir("/")) != 0:
		raise Exception("Failed to delete folder")
	return 1

def FileStat():
	fs = vsfs.Vsfs("test.vsfs", 1)
	print fs.stat("/")
	fi = fs.open("test.txt", "w")
	fi.write("foobar")
	fi.close()
	print fs.stat("/foo/test.txt")

if __name__=="__main__":
	print "CreateMultipleFiles test", CreateMultipleFiles()
	print "ReadAndWrite test", ReadAndWrite()
	print "FileHandleSync test", FileHandleSync()
	print "CreateAndDeleteFile test", CreateAndDeleteFile()
	print "CreateUseAndDeleteFolder test", CreateUseAndDeleteFolder()
	print "CreateUseAndDeleteNestedFolder test", CreateUseAndDeleteNestedFolder()

