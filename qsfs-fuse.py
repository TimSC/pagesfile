
#FUSE interfase for very simple file system

import os, stat, errno, cStringIO
import qsfs

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
		self.openCount = {}

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

		mode = "r"
		if flags & os.O_RDONLY:
			mode = "r"
		if flags & os.O_WRONLY:
			mode = "w"
		if flags & os.O_RDWR:
			mode = "rw"

		if path not in self.handles:
			try:
				handle = self.fs.open(path, mode)
			except OSError:
				return -errno.ENOENT
			self.handles[path] = handle
		if path not in self.openCount:
			self.openCount[path] = 0
		self.openCount[path] += 1
		return 0

	def read(self, path, size, offset):
		print "read", path, size, offset

		if path not in self.handles:
			return -errno.ENOENT
		handle = self.handles[path]	
		slen = len(handle)

		handle.seek(offset)
		return handle.read(size)

	def mknod(self, path, mode, dev):
		print "mknod", path, mode, dev
		handle = self.fs.open(path, "w")
		handle.write("stuff")
		print handle
		handle.close()
		del handle
		return 0

	def unlink(self, path):
		print "unlink", path
		self.fs.rm(path)
		return 0

	def release(self, path, flags):
		print "release", path, flags
		if path not in self.handles:
			print "Expected path to be already open"
		self.openCount[path] -= 1
		if self.openCount[path] == 0:
			print "Closing handle"
			del self.openCount[path]
			print self.handles[path]
			self.handles[path].close()
			del self.handles[path]
		return 0

	def flush(self, path):
		
		print "flush", path
		if path not in self.handles:
			return -errno.ENOENT
		handle = self.handles[path]	
		handle.flush()
		return 0

	def utimens(self, path, accessTime, modTime):
		print "utimens", path, accessTime, modTime

	def mythread(self):
		print '*** mythread'
		return -errno.ENOSYS

	def chmod(self, path, mode):
		print '*** chmod', path, oct(mode)
		return -errno.ENOSYS

	def chown(self, path, uid, gid):
		print '*** chown', path, uid, gid
		return -errno.ENOSYS

	def fsync(self, path, isFsyncFile):
		print '*** fsync', path, isFsyncFile
		return -errno.ENOSYS

	def link(self, targetPath, linkPath):
		print '*** link', targetPath, linkPath
		return -errno.ENOSYS

	def mkdir(self, path, mode):
		print 'mkdir', path, oct(mode)
		self.fs.mkdir(path, mode)
		return 0

	def readlink(self, path):
		print 'readlink', path
		return -errno.ENOSYS

	def rename(self, oldPath, newPath):
		print 'rename', oldPath, newPath
		self.fs.rename(oldPath, newPath)
		return 0

	def rmdir(self, path):
		print 'rmdir', path
		self.fs.rmdir(path)
		return 0

	def statfs(self):
		print 'statfs'
		return -errno.ENOSYS

	def symlink(self, targetPath, linkPath):
		print 'symlink', targetPath, linkPath
		return -errno.ENOSYS

	def truncate (self, path, size):
		print 'truncate', path, size
		return -errno.ENOSYS

	def utime (self, path, times):
		print 'utime', path, times
		return -errno.ENOSYS

	def create(self, *args):
		print "create"
		return -errno.ENOSYS

	def opendir(self, *args):
		print "opendir", args
		return 0

	def releasedir(self, *args):
		print "releasedir"
		return 0

	def fsyncdir(self, *args):
		print "fsyncdir", args
		return -errno.ENOSYS

	def fgetattr(self, *args):
		print "fgetattr", args
		return -errno.ENOSYS

	def ftruncate(self, *args):
		print "ftruncate", args
		return -errno.ENOSYS

	def getxattr(self, *args):
		print "getxattr", args
		return -errno.ENOSYS

	def listxattr(self, *args):
		print "listxattr", args
		return -errno.ENOSYS

	def setxattr(self, *args):
		print "setxattr", args
		return -errno.ENOSYS

	def removexattr(self):
		print "removexattr", args
		return -errno.ENOSYS

	def access(self, *args):
		print "access", args
		return -errno.ENOSYS

	def lock(self, *args, **argDict):
		print "lock", args, argDict
		return -errno.ENOSYS

	def bmap(self, *args):
		print "bmap", args
		return -errno.ENOSYS

	def fsinit(self, *args):
		print "fsinit", args
		return -errno.ENOSYS

	def fsdestroy(self, *args):
		print "fsdestroy", args
		return -errno.ENOSYS

def main():
	#fs = qsfs.Qsfs("test.qsfs")
	fs = qsfs.Qsfs(cStringIO.StringIO(), 1)
	fi = fs.open("test.txt","w")
	fi.write("foobar\n")
	fi.close()
	del fi
	
	server = VsfsFuse(fs)
	server.parse(errex=1)
	server.main()

if __name__ == '__main__':
	main()

