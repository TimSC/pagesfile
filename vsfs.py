import struct, os, math

def FindLargestFreeSpace(dataBitmap, atLeastBlockSize = None):
	if not isinstance(dataBitmap, bytearray):
		raise RuntimeError("Expecting bytearray")

	bestRunStart = None
	bestRunSize = 0
	currentRunStart = None
	currentRunSize = 0
	pos = 0
	
	for byteVal in dataBitmap:
		for bitNum in range(8):
			bitVal = (byteVal & (0x01 << bitNum)) != 0
			if bitVal == 0:
				if currentRunStart is None:
					currentRunStart = pos
					currentRunSize = 0
				currentRunSize += 1
			if bitVal == 1:
				currentRunStart = None
				currentRunSize = 0

			if currentRunSize > bestRunSize:
				bestRunStart = currentRunStart
				bestRunSize = currentRunSize
				if bestRunSize >= atLeastBlockSize:
					return bestRunStart, bestRunSize

			pos += 1

	return bestRunStart, bestRunSize #Data block number

def FindLooseBlocks(dataBitmap, numBlocks, storageSize, preAllocateBlocksStart = None, preAllocateBlocksSize = None):
	if not isinstance(dataBitmap, bytearray):
		raise RuntimeError("Expecting bytearray")
	pos = 0
	freeBlocks = []
	for byteVal in dataBitmap:
		for bitNum in range(8):
			bitVal = (byteVal & (0x01 << bitNum)) != 0
			if bitVal == 0:
				if preAllocateBlocksStart != None:
					if (pos < preAllocateBlocksStart or pos >= (preAllocateBlocksStart+preAllocateBlocksSize)):
						freeBlocks.append(pos)
				else:
					freeBlocks.append(pos)

			pos += 1
			if len(freeBlocks) >= numBlocks:
				return freeBlocks
			if pos >= storageSize:
				return freeBlocks

	return freeBlocks

class Vsfs(object):
	#A very simple file system in pure python
	#Inspired by "Operating Systems: Three Easy Pieces", 
	#by Remzi H. Arpaci-Dusseau and Andrea C. Arpaci-Dusseau, Chapter 40
	#http://pages.cs.wisc.edu/~remzi/OSTEP/file-implementation.pdf

	def __init__(self, fi, initFs = 0, maxFiles = 1000000, dataSize = 4096 * 4096, blockSize = 4096,
		maxFileSize = 10*1024, maxFilenameLen = 256):
		
		createFile = False
		self.freeVal = struct.unpack(">Q", "\xff\xff\xff\xff\xff\xff\xff\xff")[0]
		self.inodeMetaStruct = struct.Struct(">BQ") #Type, size
		self.inodePtrStruct = struct.Struct(">Q")
		self.folderEntryStruct = struct.Struct(">BQ") #In use, child inode number

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

			self.numInodePointers = int(math.ceil(float(maxFileSize) / blockSize))
			self.inodeEntrySize = self.inodeMetaStruct.size + self.numInodePointers * self.inodePtrStruct.size

			self.inodeBitmapStart = 1 #Starting block num
			self.sizeBlocksInodeBitmap = int(math.ceil(math.ceil(maxFiles / 8.) / blockSize))
			self.dataBitmapStart = self.inodeBitmapStart + self.sizeBlocksInodeBitmap #Block num
			self.sizeDataBlocks = int(math.ceil(float(dataSize) / blockSize)) #Blocks to contain actual data
			self.sizeBlocksDataBitmap = int(math.ceil(math.ceil(float(self.sizeDataBlocks) / 8.) / blockSize)) #Blocks to contain data bitmap

			self.inodeTableStart = self.dataBitmapStart + self.sizeBlocksDataBitmap #Block num
			self.inodeTableSizeBytes = self.inodeEntrySize * maxFiles
			self.sizeInodeTableBlocks = int(math.ceil(float(self.inodeTableSizeBytes) / blockSize))

			self.dataStart = self.inodeTableStart + self.sizeInodeTableBlocks

			self.folderEntrySize = self.folderEntryStruct.size + self.maxFilenameLen

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
		self.handle.write("".join(["\x00" for i in range(self.blockSize)]))
		self.handle.seek(0)
		self.handle.write("vsfs")

	def _update_fs_data(self):
		self.handle.seek(4)
		self.handle.write(struct.pack(">Q", self.blockSize))
		self.handle.write(struct.pack(">Q", self.maxFilenameLen))

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
		for blockNum in range(self.inodeBitmapStart, self.inodeBitmapStart + self.sizeBlocksInodeBitmap):
			self.handle.seek(blockNum * self.blockSize)
			self.handle.write("".join(["\x00" for i in range(self.blockSize)]))

		#Format data bitmap
		for blockNum in range(self.dataBitmapStart, self.dataBitmapStart + self.sizeBlocksDataBitmap):
			self.handle.seek(blockNum * self.blockSize)
			for i in range(self.blockSize):
				self.handle.write("".join(["\x00" for i in range(self.blockSize)]))

		#Create root directory
		self._create_folder(None, None)

	def _create_inode(self, inodeNum, inodeType, fileSize, inFolderInode):

		if inodeNum == 0: 
			if inFolderInode != None:
				raise RuntimeError("Inode 0 is root folder")
			if inodeType != 1:
				raise RuntimeError("Inode 0 must be a folder")

		if inodeType == 1 and fileSize != 0:
			raise RuntimeError("Folders must be created with zero filesize")

		#Check size of inode structures
		maxInodeNum = self._get_max_inode_number()
		if inodeNum > maxInodeNum:
			raise RuntimeError("Inode number too large")
		if inodeNum < 0:
			raise RuntimeError("Inode number is negative")
		bitmapByte = inodeNum / 8
		bitmapByteOffset = inodeNum % 8
		inodeEntryOffset = self.inodeEntrySize * inodeNum
			
		filePos = self.inodeBitmapStart * self.blockSize + bitmapByte
		self.handle.seek(filePos)
		bitmapVal = self.handle.read(1)
		bitmapVal = ord(bitmapVal[0]) #Convert to number
		inodeExists = (bitmapVal & (0x01 << bitmapByteOffset)) != 0
		if inodeExists:
			raise RuntimeError("Inode already exists")

		#Update inode bitmap
		updatedBitmapVal = bitmapVal | (0x01 << bitmapByteOffset)
		self.handle.seek(filePos)
		bitmapVal = self.handle.write(chr(updatedBitmapVal))

		#Clear inode entry
		inodeEntryPos = inodeEntryOffset + self.inodeTableStart * self.blockSize
		self.handle.seek(inodeEntryPos)
		self.handle.write("".join(["\xff" for i in range(self.inodeEntrySize)]))
		
		#Write into inode table
		self.handle.seek(inodeEntryPos)
		self.handle.write(self.inodeMetaStruct.pack(inodeType, fileSize))

	def _load_inode(self, inodeNum):

		#Check size of inode structures
		maxInodeNum = self._get_max_inode_number()
		if inodeNum > maxInodeNum:
			raise RuntimeError("Inode number too large")
		if inodeNum < 0:
			raise RuntimeError("Inode number is negative")

		inodeEntryPos = self.inodeTableStart * self.blockSize + inodeNum * self.inodeEntrySize
		self.handle.seek(inodeEntryPos)
		inodeRaw = self.handle.read(self.inodeEntrySize)

		inodeType, fileSize = self.inodeMetaStruct.unpack(inodeRaw[:self.inodeMetaStruct.size])
		meta = {}
		meta["inodeType"] = inodeType
		meta["fileSize"] = fileSize

		self.inodeMetaStruct.size + self.numInodePointers * self.inodePtrStruct.size

		dataPtrs = []
		for ptrNum in range(self.numInodePointers):
			ptrOffset = self.inodeMetaStruct.size + ptrNum * self.inodePtrStruct.size
			ptrVal = self.inodePtrStruct.unpack(inodeRaw[ptrOffset:ptrOffset+self.inodePtrStruct.size])[0]
			if ptrVal != self.freeVal:
				dataPtrs.append(ptrVal)
			else:
				dataPtrs.append(None)
		return meta, dataPtrs

	def _update_inode(self, inodeNum, meta, ptrs):
		inodeEntryOffset = self.inodeEntrySize * inodeNum

		inodeEntryPos = inodeEntryOffset + self.inodeTableStart * self.blockSize
		self.handle.seek(inodeEntryPos)
		
		self.handle.seek(inodeEntryPos)
		self.handle.write(self.inodeMetaStruct.pack(meta['inodeType'], meta['fileSize']))

		for ptrNum, ptr in enumerate(ptrs):
			if ptr != None:
				self.handle.write(struct.pack(">Q", ptr))
			else:
				self.handle.write(struct.pack(">Q", self.freeVal))

	def _get_max_inode_number(self):
		#Check size of inode structures
		bitmapCapacity = self.sizeBlocksInodeBitmap * self.blockSize * 8
		tableCapacity = (self.sizeInodeTableBlocks * self.blockSize / self.inodeEntrySize) - 1
		if bitmapCapacity < tableCapacity:
			return bitmapCapacity
		return tableCapacity #This number is one less than the number of capacity slots

	def _read_folder_block(self, blkNum):

		out = []
		self.handle.seek(self.dataStart + blkNum * self.blockSize)
		folderBlockData = self.handle.read(self.blockSize)
		numberOfEntries = self.blockSize / self.folderEntrySize
		for entryNum in range(numberOfEntries):
			datOffset = entryNum * self.folderEntrySize
			nameOffset = datOffset + self.folderEntryStruct.size

			inUse, inodeNum = self.folderEntryStruct.unpack(folderBlockData[datOffset:nameOffset])
			nameDat = folderBlockData[nameOffset:nameOffset+self.maxFilenameLen]
			out.append([inUse, inodeNum, nameDat.decode('utf-8')])
		return out

	def _create_file(self, filename, fileSize, parentFolderInodeNum):

		#Check parent folder can fit another file
		parentFolderMeta, parentFolderPtrs = self._load_inode(parentFolderInodeNum)
		if parentFolderMeta['inodeType'] != 1:
			raise RuntimeError("Parent inode must be a folder")

		freeFolderDataBlk = None
		freeEntryNum = None
		for ptrNum, ptr in enumerate(parentFolderPtrs):
			if ptr is None: continue
			folderBlockList = self._read_folder_block(ptr)
			for entryNum, (inUse, inode, name) in enumerate(folderBlockList):
				if inUse: continue

				freeFolderDataBlk = ptr
				freeEntryNum = entryNum

			if freePtr is not None:
				break

		if freeFolderDataBlk is None:
			#Try to allocate more space to keep folder data
			allocatedBlocks = self._allocate_space_to_inode(parentFolderInodeNum, 1)

			#Clear new blocks
			for blkNum in allocatedBlocks:
				self.handle.seek(self.dataStart + blkNum * self.blockSize)
				self.handle.write("".join(["\x00" for i in range(self.blockSize)]))

			freeFolderDataBlk = allocatedBlocks[0]
			freeEntryNum = 0

		if freeFolderDataBlk is None:
			raise RuntimeError("Folder has reached the maximum number of files")

		#Preallocate blocks
		self.handle.seek(self.dataBitmapStart * self.blockSize)
		dataBitmap = bytearray(self.handle.read(self.sizeBlocksDataBitmap * self.blockSize))

		requiredFreeBlocks = int(math.ceil(float(fileSize) / self.blockSize))

		dataBlockNums = []
		preAllocateBlocksStart, preAllocateBlocksSize = FindLargestFreeSpace(dataBitmap, requiredFreeBlocks)
		if preAllocateBlocksStart is not None:
			dataBlockNums = range(preAllocateBlocksStart, preAllocateBlocksStart+preAllocateBlocksSize)

		if len(dataBlockNums) < requiredFreeBlocks:
			extraBlocks = FindLooseBlocks(dataBitmap, requiredFreeBlocks - len(dataBlockNums), self.sizeDataBlocks, 
				preAllocateBlocksStart, preAllocateBlocksSize)
			dataBlockNums.extend(extraBlocks)

		#Create inode
		
		#Set inode pointers

		#Update parent folder

		pass

	def _allocate_space_to_inode(self, inodeNum, blocksToAdd):
		meta, ptrs = self._load_inode(inodeNum)
		
		#Find free pointer
		freePtrNums = []
		for i, ptr in enumerate(ptrs):
			if ptr == None:
				freePtrNums.append(i)
			if len(freePtrNums) >= blocksToAdd:
				break
		
		if len(freePtrNums) < blocksToAdd:
			raise RuntimeError("Insufficient pointer space")

		#Get a free block
		self.handle.seek(self.dataBitmapStart * self.blockSize)
		dataBitmap = bytearray(self.handle.read(self.sizeBlocksDataBitmap * self.blockSize))
		freeBlocks = FindLooseBlocks(dataBitmap, blocksToAdd, self.sizeDataBlocks)

		if len(freeBlocks) < blocksToAdd:
			raise RuntimeError("Insufficient blocks available")

		for i, blk in enumerate(freeBlocks):
			#Update data bitmap
			bitmapByte = blk / 8
			bitmapByteOffset = blk % 8
		
			filePos = self.dataBitmapStart * self.blockSize + bitmapByte
			byteVal = dataBitmap[bitmapByte]
			bitVal = (byteVal & (0x01 << bitmapByteOffset)) != 0
			if bitVal:
				raise RuntimeError("Interal error, bitval should be zero")
			updatedByteVal = byteVal | (0x01 << bitmapByteOffset)
			self.handle.seek(filePos)
			self.handle.write(chr(updatedByteVal))
			dataBitmap[bitmapByte] = chr(updatedByteVal)

			#Update inode entry in memory
			ptrs[freePtrNums[i]] = blk

		#Update inode entry on disk
		self._update_inode(inodeNum, meta, ptrs)
		return freeBlocks

	def _create_folder(self, foldername, inFolderInode):
		print "_create_folder"

		if foldername == None:
			if inFolderInode != None:
				raise RuntimeError("Root folder has no parent folder")

		#Allocate a free inode
		if foldername == None:
			folderInodeNum = 0
		else:
			self.handle.seek(self.inodeBitmapStart * self.blockSize)
			inodeBitmap = self.handle.read(self.sizeBlocksInodeBitmap * self.blockSize)

			maxInodeNum = self._get_max_inode_number()
			freeInodeNums = FindLooseBlocks(inodeBitmap, 1, maxInodeNum+1)
			if len(freeInodeNums) == 0:
				raise RuntimeError("Inode bitmap full")

			folderInodeNum = freeInodeNums[0]

		self._create_inode(folderInodeNum, 1, 0, None)		

		allocatedBlocks = self._allocate_space_to_inode(folderInodeNum, 1)

		#Clear new blocks
		for blkNum in allocatedBlocks:
			self.handle.seek(self.dataStart + blkNum * self.blockSize)
			self.handle.write("".join(["\x00" for i in range(self.blockSize)]))

	def open(self, filename, mode):
		pass

	

