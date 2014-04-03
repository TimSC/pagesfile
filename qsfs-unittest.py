
import qsfs, os

def CreateMultipleFiles():
	fs = qsfs.Qsfs("test.qsfs", 1)
	for i in range(30):
		fi = fs.open("test{0}".format(i),"w")
	found = fs.listdir("/")
	if len(found) != 30:
		os.unlink("test.qsfs")
		raise Exception("Wrong number of files")
	for i in range(30):
		if "test{0}".format(i) not in found:
			os.unlink("test.qsfs")
			raise Exception("Missing file")
	os.unlink("test.qsfs")
	return 1

def ReadAndWrite():
	fs = qsfs.Qsfs("test.qsfs", 1)

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

	fs = qsfs.Qsfs("test.qsfs")
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
	fs = qsfs.Qsfs("test.qsfs", 1)

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
	fs = qsfs.Qsfs("test.qsfs", 1)

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
	fs = qsfs.Qsfs("test.qsfs", 1)
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
	fs = qsfs.Qsfs("test.qsfs", 1)
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

def DeleteInUseThings():
	fs = qsfs.Qsfs("test.qsfs", 1)
	fs.mkdir("/foo", 0)
	if len(fs.listdir("/")) != 1:
		raise Exception("Failed to create folder")

	fi = fs.open("/foo/test.txt", "w")
	fi.write("foobar")

	#This should fail
	failed = False
	try:
		fs.rm("/foo/test.txt")
	except OSError:
		failed = True
	if not failed:
		raise Exception("This should throw an exception")

	#This should fail	
	failed = False
	try:
		fs.rmdir("/foo")
	except OSError:
		failed = True
	if not failed:
		raise Exception("This should throw an exception")

	fi.close()
	return 1

def RenameFile():
	fs = qsfs.Qsfs("test.qsfs", 1)

	fi = fs.open("test.txt", "w")
	fi.write("foobar")
	fi.close()

	fs.rename("/test.txt", "/stuff.txt")

	d = fs.listdir("/")
	if len(d)!=1:
		raise Exception("Wrong number of files")	
	if d[0]!="stuff.txt":
		raise Exception("Rename failed")	

	return 1

def FileStat():
	fs = qsfs.Qsfs("test.qsfs", 1)
	print fs.stat("/")
	fi = fs.open("test.txt", "w")
	fi.write("foobar")
	fi.close()
	print fs.stat("/foo/test.txt")
	return 1

def UnitTests():
	print "CreateMultipleFiles test", CreateMultipleFiles()
	print "ReadAndWrite test", ReadAndWrite()
	print "FileHandleSync test", FileHandleSync()
	print "CreateAndDeleteFile test", CreateAndDeleteFile()
	print "CreateUseAndDeleteFolder test", CreateUseAndDeleteFolder()
	print "CreateUseAndDeleteNestedFolder test", CreateUseAndDeleteNestedFolder()
	print "DeleteInUseThings test", DeleteInUseThings()
	print "RenameFile test", RenameFile()

if __name__=="__main__":
	UnitTests()	
	

