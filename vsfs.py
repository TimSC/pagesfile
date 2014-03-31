import struct, os, math

class Vsfs(object):
	def __init__(self, fi, initFs = 0, maxFiles = 1000000, dataSize = 4096 * 4096, blockSize = 4096,
		maxFileSize = 1024*1024, maxFilenameLen = 256):
		
		createFile = False
		if isinstance(fi, str):
			createFile = not os.path.isfile(fi)
			self.filename = fi
			if createFile:
				self.handle = open(fi, "w+b")
			else:
				self.handle = open(fi, "r+b")
			self.haveFileOwnership = True
		else:
			self.handle = fi
			self.haveFileOwnership = False

		if createFile or initFs:
			self.maxFiles = maxFiles
			self.dataSize = dataSize
			self.blockSize = blockSize
			self.maxFilenameLen = maxFilenameLen

			#self.superBlockStruct = struct.Struct("")
			self.inodeMetaStruct = struct.Struct(">BQ") #Type, size
			self.inodePtrStruct = struct.Struct(">Q")

			self.numInodePointers = int(math.ceil(float(maxFileSize) / blockSize))
			self.inodeEntrySize = self.inodeMetaStruct.size + self.numInodePointers * self.inodePtrStruct.size + self.maxFilenameLen

			self.inodeBitmapStart = 1 #Block num
			self.sizeBlocksInodeBitmap = int(math.ceil(math.ceil(maxFiles / 8.) / blockSize))
			self.dataBitmapStart = self.inodeBitmapStart + self.sizeBlocksInodeBitmap #Block num
			self.sizeDataBlocks = int(math.ceil(float(dataSize) / blockSize)) #Blocks to contain actual data
			self.sizeBlocksDataBitmap = int(math.ceil(math.ceil(self.sizeDataBlocks / 8.) / blockSize)) #Blocks to contain data bitmap

			self.inodeTableStart = self.dataBitmapStart + self.sizeBlocksDataBitmap #Block num
			self.inodeTableSizeBytes = self.inodeEntrySize * maxFiles
			self.sizeInodeTableBlocks = int(math.ceil(self.inodeTableSizeBytes/ blockSize))

			self.dataStart = self.inodeTableStart + self.sizeInodeTableBlocks

			self._init_superblock()
			self._quick_format()
			self._update_fs_data()

		else:
			#Read settings
			pass




	def __del__(self):
		pass

	def _print_layout(self):
		print "Superblock 0"
		print "inodeBitmapStart", self.inodeBitmapStart
		print "dataBitmapStart", self.dataBitmapStart
		print "inodeTableStart", self.inodeTableStart
		print "dataStart", self.dataStart

	def _init_superblock(self):
		self.handle.seek(0)
		for i in range(self.blockSize):
			self.handle.write("\x00")
		self.handle.seek(0)
		self.handle.write("vsfs")

	def _update_fs_data(self):
		self.handle.seek(4)
		self.handle.write(struct.pack(">Q", self.blockSize))

		#Fundamental layout
		self.handle.write(struct.pack(">Q", self.inodeBitmapStart))
		self.handle.write(struct.pack(">Q", self.sizeBlocksInodeBitmap))

		self.handle.write(struct.pack(">Q", self.dataBitmapStart))
		self.handle.write(struct.pack(">Q", self.sizeBlocksDataBitmap))

		self.handle.write(struct.pack(">Q", self.inodeTableStart))
		self.handle.write(struct.pack(">Q", self.sizeInodeTableBlocks))

		self.handle.write(struct.pack(">Q", self.dataStart))
		self.handle.write(struct.pack(">Q", self.sizeDataBlocks))
		
	def _quick_format(self):

		#Format Inode bitmap
		for blockNum in range(self.inodeBitmapStart, self.dataBitmapStart):
			self.handle.seek(blockNum * self.blockSize)
			for i in range(self.blockSize):
				self.handle.write("\x00")

		#Format data bitmap
		for blockNum in range(self.dataBitmapStart, self.inodeTableStart):
			self.handle.seek(blockNum * self.blockSize)
			for i in range(self.blockSize):
				self.handle.write("\x00")

	def Open(filename, mode):
		pass

	

