
#FUSE interfase for very simple file system

import os, stat, errno
import vsfs

try:
	import _find_fuse_parts
except ImportError:
	pass
import fuse
from fuse import Fuse

if not hasattr(fuse, '__version__'):
	raise RuntimeError("your fuse-py doesn't know of fuse.__version__, probably it's too old.")

fuse.fuse_python_api = (0, 2)

class VsfsFuse(Fuse):

	def __init__(self, fs):
		Fuse.__init__(self)
		self.fs = fs
		self.handles = {}

	def getattr(self, path):
		print "getattr", path
		try:
			result = self.fs.stat(path)
			fuseStat = fuse.Stat()
			for key in result.__dict__:
				setattr(fuseStat, key, getattr(result, key))
			return fuseStat

		except OSError:
			return -errno.ENOENT
		return -errno.ENOENT

	def readdir(self, path, offset):
		#print "readdir", path, offset
		folderContent = [".", ".."]
		folderContent.extend(map(str, self.fs.listdir(path)))

		for r in folderContent:
			yield fuse.Direntry(r)

	def open(self, path, flags):
		print "open", path, flags

		if path not in self.handles:
			try:
				handle = self.fs.open(path, "r")
			except OSError:
				return -errno.ENOENT
			self.handles[path] = handle

		accmode = os.O_RDONLY | os.O_WRONLY | os.O_RDWR
		if (flags & accmode) != os.O_RDONLY:
			return -errno.EACCES

	def read(self, path, size, offset):
		print "read", path, size, offset

		if path not in self.handles:
			return -errno.ENOENT
		handle = self.handles[path]	
		slen = len(handle)

		handle.seek(offset)
		return handle.read(size)

def main():
	fs = vsfs.Vsfs("test.vsfs")
	fi = fs.open("test.txt","w")
	fi.write("foobar\n")
	fi.close()
	del fi
	
	server = VsfsFuse(fs)
	server.parse(errex=1)
	server.main()

if __name__ == '__main__':
	main()

