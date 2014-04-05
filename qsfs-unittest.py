
import qsfs, os

def CreateMultipleFiles(factory):
	fs = factory()
	for i in range(30):
		fi = fs.open("test{0}".format(i),"w")
	found = fs.listdir("/")
	if len(found) != 30:
		try:
			os.unlink("test.qsfs")
		except:
			pass
		raise Exception("Wrong number of files")
	for i in range(30):
		if "test{0}".format(i) not in found:
			os.unlink("test.qsfs")
			raise Exception("Missing file")
	try:
		os.unlink("test.qsfs")
	except:
		pass
	return 1

def ReadAndWrite(factory):
	fs = factory()

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

	fs = factory()
	#fs._print_layout()

	fi2 =  fs.open("test1","r")
	testStr2 = fi2.read(8000)
	count = 0
	for a, b in zip(testStr, testStr2):
		count += 1
	if count != 6000:
		raise Exception("Read back failed (2)")

	return 1

def FileHandleSync(factory):
	fs = factory()

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

def CreateAndDeleteFile(factory):
	fs = factory()

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

def CreateUseAndDeleteFolder(factory):
	fs = factory()
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

def CreateUseAndDeleteNestedFolder(factory):
	fs = factory()
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

def DeleteInUseThings(factory):
	fs = factory()
	fs.mkdir("/foo", 0)
	if len(fs.listdir("/")) != 1:
		raise Exception("Failed to create folder")

	fi = fs.open("/foo/test.txt", "w")
	fi.write("foobar")

	#This should fail
	failed = False
	try:
		fs.rm("/foo/test.txt")
	except Exception:
		failed = True
	if not failed:
		raise Exception("This should throw an exception")

	#This should fail	
	failed = False
	try:
		fs.rmdir("/foo")
	except Exception:
		failed = True
	if not failed:
		raise Exception("This should throw an exception")

	fi.close()
	return 1

def RenameFile(factory):
	fs = factory()

	fi = fs.open("test.txt", "w")
	fi.write("foobar")
	fi.close()

	fs.mv("/test.txt", "/stuff.txt")

	d = fs.listdir("/")
	if len(d)!=1:
		raise Exception("Wrong number of files")	
	if d[0]!="stuff.txt":
		raise Exception("Rename failed")	

	return 1

def MoveFile(factory):
	fs = factory()

	fi = fs.open("test.txt", "w")
	fi.write("foobar")
	fi.close()

	fs.mkdir("/foo")

	fs.mv("/test.txt", "/foo/stuff.txt")

	d = fs.listdir("/")
	if len(d)!=1:
		raise Exception("Wrong number of files: {0}".format(len(d)))	

	d = fs.listdir("/foo")
	if len(d)!=1:
		raise Exception("Wrong number of files: {0}".format(len(d)))	

	if d[0]!="stuff.txt":
		raise Exception("Rename failed")	

	return 1

def FileStat(factory):
	fs = factory()
	print fs.stat("/")
	fi = fs.open("test.txt", "w")
	fi.write("foobar")
	fi.close()
	print fs.stat("/foo/test.txt")
	return 1

def FactoryFileStore():
	return qsfs.Qsfs("test.qsfs", 1)

def FactoryStringIO():
	import cStringIO
	return qsfs.Qsfs(cStringIO.StringIO(), 1)

def UnitTests():
	if 1:
		factory = FactoryFileStore
		reloadable = 1

	if 0:
		factory = FactoryStringIO
		reloadable = 0

	print "CreateMultipleFiles test", CreateMultipleFiles(factory)
	if reloadable:
		print "ReadAndWrite test", ReadAndWrite(factory)
	print "FileHandleSync test", FileHandleSync(factory)
	print "CreateAndDeleteFile test", CreateAndDeleteFile(factory)
	print "CreateUseAndDeleteFolder test", CreateUseAndDeleteFolder(factory)
	print "CreateUseAndDeleteNestedFolder test", CreateUseAndDeleteNestedFolder(factory)
	print "DeleteInUseThings test", DeleteInUseThings(factory)
	print "RenameFile test", RenameFile(factory)
	print "MoveFile test", MoveFile(factory)

if __name__=="__main__":
	UnitTests()	
	

