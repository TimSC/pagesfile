import struct, os, math

#******************** Utility functions *********************

def FindLargestFreeSpace(dataBitmap, atLeastBlockSize = None):
	if not isinstance(dataBitmap, bytearray):
		raise TypeError("Expecting bytearray")

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
		raise TypeError("Expecting bytearray")
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

#******************* Very simple file system *******************

class Vsfs(object):
	#A very simple file system in pure python
	#Inspired by "Operating Systems: Three Easy Pieces", 
	#by Remzi H. Arpaci-Dusseau and Andrea C. Arpaci-Dusseau, Chapter 40
	#http://pages.cs.wisc.edu/~remzi/OSTEP/file-implementation.pdf

	def __init__(self, fi, initFs = 0, maxFiles = 1000000, deviceSize = 10 * 4096 * 4096, blockSize = 4096,
		maxFileSize = 10*1024, maxFilenameLen = 256):
		
		createFile = False
		self.debugMode = False
		self.freeVal = struct.unpack(">Q", "\xff\xff\xff\xff\xff\xff\xff\xff")[0]
		self.inodeMetaStruct = struct.Struct(">BQ") #Type, size
		self.inodePtrStruct = struct.Struct(">Q")
		self.folderEntryStruct = struct.Struct(">BBQ") #In use, filename length, child inode number

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
			self.deviceSize = deviceSize
			self.blockSize = blockSize
			self.maxFilenameLen = maxFilenameLen
			self.maxFileSize = maxFileSize

			#Determine size of structures
			self.numInodePointers = int(math.ceil(float(self.maxFileSize) / blockSize))
			self.inodeEntrySize = self.inodeMetaStruct.size + self.numInodePointers * self.inodePtrStruct.size
			self.sizeBlocksInodeBitmap = int(math.ceil(math.ceil(maxFiles / 8.) / blockSize))
			self.sizeDataBlocksPlanned = int(math.ceil(float(deviceSize) / blockSize)) #Blocks to contain actual data
			self.sizeBlocksDataBitmap = int(math.ceil(math.ceil(float(self.sizeDataBlocksPlanned) / 8.) / blockSize)) #Blocks to contain data bitmap
			inodeTableSizeBytes = self.inodeEntrySize * maxFiles
			self.sizeInodeTableBlocks = int(math.ceil(float(inodeTableSizeBytes) / blockSize))

			#Detemine layout of file system
			self.inodeBitmapStart = 1 #Starting block num
			self.dataBitmapStart = self.inodeBitmapStart + self.sizeBlocksInodeBitmap #Block num
			self.inodeTableStart = self.dataBitmapStart + self.sizeBlocksDataBitmap #Block num
			self.dataStart = self.inodeTableStart + self.sizeInodeTableBlocks

			#Allocate remaining space to user data
			self.sizeDataBlocks = int(math.ceil(float(deviceSize) / blockSize)) - self.dataStart #Blocks to contain actual data
			if self.sizeDataBlocks <= 0:
				raise RuntimeError("No space for user data: try reducing maximum number of files")

			self.folderEntrySize = self.folderEntryStruct.size + self.maxFilenameLen

			self._init_superblock()
			self._quick_format()
			self._update_superblock_data()

		else:
			#Read settings
			self._read_superblock_data()
			self.numInodePointers = int(math.ceil(float(self.maxFileSize) / blockSize))
			self.inodeEntrySize = self.inodeMetaStruct.size + self.numInodePointers * self.inodePtrStruct.size
			self.folderEntrySize = self.folderEntryStruct.size + self.maxFilenameLen


	def __del__(self):
		self.flush()

	def flush(self):
		self.handle.flush()

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

	def _update_superblock_data(self):
		self.handle.seek(4)

		#Main parameters
		self.handle.write(struct.pack(">Q", self.blockSize))
		self.handle.write(struct.pack(">Q", self.maxFilenameLen))
		self.handle.write(struct.pack(">Q", self.deviceSize))
		self.handle.write(struct.pack(">Q", self.maxFileSize))

		#Fundamental layout
		self.handle.write(struct.pack(">Q", self.inodeBitmapStart))
		self.handle.write(struct.pack(">Q", self.sizeBlocksInodeBitmap))

		self.handle.write(struct.pack(">Q", self.dataBitmapStart))
		self.handle.write(struct.pack(">Q", self.sizeBlocksDataBitmap))

		self.handle.write(struct.pack(">Q", self.inodeTableStart))
		self.handle.write(struct.pack(">Q", self.sizeInodeTableBlocks))

		self.handle.write(struct.pack(">Q", self.dataStart))
		self.handle.write(struct.pack(">Q", self.sizeDataBlocks))

		#Check if we have not run out of the first block
		currentPos = self.handle.tell()
		if currentPos > self.blockSize:
			raise RuntimeError("Superblock is bigger than specified block size")
	
	def _read_superblock_data(self):
		self.handle.seek(0)
		fsIdent = self.handle.read(4)
		if fsIdent != "vsfs":
			raise IOError("Unrecognised identifier for vsfs file system")

		#Main parameters
		self.blockSize = struct.unpack(">Q", self.handle.read(8))[0]
		self.maxFilenameLen = struct.unpack(">Q", self.handle.read(8))[0]
		self.deviceSize = struct.unpack(">Q", self.handle.read(8))[0]
		self.maxFileSize = struct.unpack(">Q", self.handle.read(8))[0]

		#Fundamental layout
		self.inodeBitmapStart = struct.unpack(">Q", self.handle.read(8))[0]
		self.sizeBlocksInodeBitmap = struct.unpack(">Q", self.handle.read(8))[0]

		self.dataBitmapStart = struct.unpack(">Q", self.handle.read(8))[0]
		self.sizeBlocksDataBitmap = struct.unpack(">Q", self.handle.read(8))[0]

		self.inodeTableStart = struct.unpack(">Q", self.handle.read(8))[0]
		self.sizeInodeTableBlocks = struct.unpack(">Q", self.handle.read(8))[0]

		self.dataStart = struct.unpack(">Q", self.handle.read(8))[0]
		self.sizeDataBlocks = struct.unpack(">Q", self.handle.read(8))[0]

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

	def _create_inode(self, inodeNum, inodeType, fileSize):
		#print "_create_inode"
		if inodeNum == 0: 

			if inodeType != 1:
				raise ValueError("Inode 0 must be a folder")

		if inodeType == 1 and fileSize != 0:
			raise ValueError("Folders must be created with zero filesize")

		#Check size of inode structures
		maxInodeNum = self._get_max_inode_number()
		if inodeNum > maxInodeNum:
			raise ValueError("Inode number too large")
		if inodeNum < 0:
			raise ValueError("Inode number is negative")
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
			raise ValueError("Inode number too large")
		if inodeNum < 0:
			raise ValueError("Inode number is negative")

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
		#print "_update_inode", meta
		inodeEntryOffset = self.inodeEntrySize * inodeNum

		inodeEntryPos = inodeEntryOffset + self.inodeTableStart * self.blockSize
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
		#print "_read_folder_block", blkNum

		out = []
		self.handle.seek((self.dataStart + blkNum) * self.blockSize)
		folderBlockData = self.handle.read(self.blockSize)
		if len(folderBlockData) != self.blockSize:
			raise RuntimeError("Failed to read data block "+str(blkNum))
		numberOfEntries = self.blockSize / self.folderEntrySize
		for entryNum in range(numberOfEntries):
			datOffset = entryNum * self.folderEntrySize
			nameOffset = datOffset + self.folderEntryStruct.size

			inUse, filenameLen, inodeNum = self.folderEntryStruct.unpack(folderBlockData[datOffset:nameOffset])
			nameDat = folderBlockData[nameOffset:nameOffset+filenameLen]
			out.append([inUse, inodeNum, nameDat.decode('utf-8')])
		return out

	def _write_folder_block(self, blkNum, inodeList):
		#print "_write_folder_block", blkNum

		self.handle.seek((self.dataStart + blkNum) * self.blockSize)
		numberOfEntries = self.blockSize / self.folderEntrySize
		for entry in inodeList:

			encodedFilename = entry[2].encode('utf-8')
			if len(encodedFilename) > self.maxFilenameLen:
				raise RuntimeError("Internal error, file name too long")

			self.handle.write(self.folderEntryStruct.pack(entry[0], len(encodedFilename), entry[1]))
			self.handle.write(encodedFilename)
			self.handle.write("".join(["\x00" for i in range(self.maxFilenameLen - len(encodedFilename))]))

	def _check_folder_can_hold_another_inoid(self, parentFolderInodeNum, parentFolderPtrs):

		freeFolderBlockNum = None
		freeFolderBlockData = None
		freeEntryNum = None

		for ptrNum, ptr in enumerate(parentFolderPtrs):
			if ptr is None: continue
			folderBlockList = self._read_folder_block(ptr)
			for entryNum, (inUse, inode, name) in enumerate(folderBlockList):
				if inUse: continue
				freeFolderBlockData = folderBlockList
				freeFolderBlockNum = ptr
				freeEntryNum = entryNum

				if freeFolderBlockNum is not None:
					break

			if freeFolderBlockNum is not None:
				break

		if freeFolderBlockNum is None:
			#Try to allocate more space to keep folder data
			allocatedBlocks = self._allocate_space_to_inode(parentFolderInodeNum, 1)

			#Clear new blocks
			for blkNum in allocatedBlocks:
				self.handle.seek((self.dataStart + blkNum) * self.blockSize)
				self.handle.write("".join(["\x00" for i in range(self.blockSize)]))

			freeFolderBlockNum = allocatedBlocks[0]
			freeFolderBlockData = self._read_folder_block(freeFolderBlockNum)
			freeEntryNum = 0 #Assume we can use the first entry in a new block

		if self.debugMode:
			#Debugging tests
			testMeta, testPtrs = self._load_inode(parentFolderInodeNum)
			if testMeta['inodeType'] != 1:
				raise RuntimeError("Folder inode type corrupted")

		if freeFolderBlockNum is None:
			return 0, freeFolderBlockNum, freeFolderBlockData, freeEntryNum
		return 1, freeFolderBlockNum, freeFolderBlockData, freeEntryNum

	def _add_inode_to_folder(self, filename, childInodeNum, parentFolderInodeNum, \
		freeFolderBlockNum, freeFolderBlockData, freeEntryNum):
	
		if freeFolderBlockData[freeEntryNum][0] != 0:
			raise RuntimeError("Internal error, expected inuse flag to be zero")
		freeFolderBlockData[freeEntryNum][0] = 1
		freeFolderBlockData[freeEntryNum][1] = childInodeNum
		freeFolderBlockData[freeEntryNum][2] = filename
		self._write_folder_block(freeFolderBlockNum, freeFolderBlockData)

	def _create_file(self, filename, fileSize, parentFolderInodeNum):
		#print "_create_file", filename, fileSize, parentFolderInodeNum

		encodedFilename = filename.encode("utf-8")
		if len(encodedFilename) > self.maxFilenameLen:
			raise ValueError("Filename, when encoded into utf-8, is too long")

		#Check parent folder can fit another file
		parentFolderMeta, parentFolderPtrs = self._load_inode(parentFolderInodeNum)

		if parentFolderMeta['inodeType'] != 1:
			raise ValueError("Parent inode must be a folder")

		parentFolderOk, freeFolderBlockNum, freeFolderBlockData, freeEntryNum = \
			self._check_folder_can_hold_another_inoid(parentFolderInodeNum, parentFolderPtrs)
		if not parentFolderOk:
			raise RuntimeError("Folder has reached the maximum number of files")

		requiredFreeBlocks = int(math.ceil(float(fileSize) / self.blockSize))
		if requiredFreeBlocks > 0:
			#Preallocate blocks

			self.handle.seek(self.dataBitmapStart * self.blockSize)
			dataBitmap = bytearray(self.handle.read(self.sizeBlocksDataBitmap * self.blockSize))

			dataBlockNums = []
			preAllocateBlocksStart, preAllocateBlocksSize = FindLargestFreeSpace(dataBitmap, requiredFreeBlocks)
			if preAllocateBlocksStart is not None:
				dataBlockNums = range(preAllocateBlocksStart, preAllocateBlocksStart+preAllocateBlocksSize)

			if len(dataBlockNums) < requiredFreeBlocks:
				extraBlocks = FindLooseBlocks(dataBitmap, requiredFreeBlocks - len(dataBlockNums), self.sizeDataBlocks, 
					preAllocateBlocksStart, preAllocateBlocksSize)
				dataBlockNums.extend(extraBlocks)

		#Allocate an inode number for this file
		self.handle.seek(self.inodeBitmapStart * self.blockSize)
		dataBitmap = bytearray(self.handle.read(self.sizeBlocksInodeBitmap * self.blockSize))
		maxInodeNum = self._get_max_inode_number()
		freeInodes = FindLooseBlocks(dataBitmap, 1, maxInodeNum+1)
		if len(freeInodes) == 0:
			raise RuntimeError("Maximum number of inodes reached")
		fileInodeNum = freeInodes[0]
		
		#Create inode
		self._create_inode(fileInodeNum, 2, fileSize)

		#Set inode pointers
		if requiredFreeBlocks > 0:
			self._allocate_space_to_inode(fileInodeNum, requiredFreeBlocks)

		#Update parent folder
		self._add_inode_to_folder(filename, fileInodeNum, parentFolderInodeNum, \
			freeFolderBlockNum, freeFolderBlockData, freeEntryNum)

		return fileInodeNum

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
		#print "_create_folder", foldername, inFolderInode

		if foldername == None:
			if inFolderInode != None:
				raise ValueError("Root folder has no parent folder")

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

		self._create_inode(folderInodeNum, 1, 0)

		#print self._load_inode(folderInodeNum)

		allocatedBlocks = self._allocate_space_to_inode(folderInodeNum, 1)

		#Clear new blocks
		for blkNum in allocatedBlocks:
			self.handle.seek((self.dataStart + blkNum) * self.blockSize)
			self.handle.write("".join(["\x00" for i in range(self.blockSize)]))

	def _write_to_data_block(self, dataBlockNum, posInBlock, datToWrite):
		#print "_write_to_data_block", dataBlockNum, posInBlock

		if not isinstance(dataBlockNum, int):
			raise ValueError("dataBlockNum should be int")
		if not isinstance(posInBlock, int):
			raise ValueError("posInBlock should be int")

		if posInBlock + len(datToWrite) > self.blockSize:
			raise RuntimeError("Too much data to write to block")

		self.handle.seek((self.dataStart + dataBlockNum) * self.blockSize + posInBlock)
		self.handle.write(datToWrite)

	def _read_from_data_block(self, dataBlockNum, posInBlock, bytesToRead):
		#print "_read_from_data_block", dataBlockNum, posInBlock, bytesToRead
		if bytesToRead == 0:
			return ""

		if posInBlock + bytesToRead > self.blockSize:
			raise RuntimeError("Read requires more data than contained in block")

		self.handle.seek((self.dataStart + dataBlockNum) * self.blockSize + posInBlock)
		return self.handle.read(bytesToRead)

	def _update_file_size(self, inodeNum, newFileSize):
		inodeEntryPos = self.inodeTableStart * self.blockSize + inodeNum * self.inodeEntrySize
		inodeType = 2
		if self.debugMode:
			self.handle.seek(inodeEntryPos)
			inodeType, fileSize = self.inodeMetaStruct.unpack(self.handle.read(self.inodeMetaStruct.size))
			if inodeType != 2:
				raise RuntimeError("Internal error, unexpected inode code")
		self.handle.seek(inodeEntryPos)
		self.handle.write(self.inodeMetaStruct.pack(inodeType, newFileSize))

	def open(self, filename, mode):

		#Get folder inode
		folderMeta, folderPtrs = self._load_inode(0)
		if folderMeta['inodeType'] != 1:
			raise ValueError("Not a folder")	

		#For each data block

		for ptr in folderPtrs:
			if ptr is None:
				continue
			folderBlock = self._read_folder_block(ptr)
			for entry in folderBlock:
				if entry[0] == 0: 
					continue #Not in use
				if entry[2] == filename:
					return VsfsFile(entry[1], self, mode)

		if mode == "r":
			raise IOError("File not found")

		#Create new file
		parentFolder = 0
		fileInode = self._create_file(filename, 0, parentFolder)
		return VsfsFile(fileInode, self, mode)

	def listdir(self, path):
		#Get folder inode
		folderMeta, folderPtrs = self._load_inode(0)
		if folderMeta['inodeType'] != 1:
			raise ValueError("Not a folder")	

		#For each data block
		out = []
		for ptr in folderPtrs:
			if ptr is None:
				continue
			folderBlock = self._read_folder_block(ptr)
			for entry in folderBlock:
				if entry[0] == 0: 
					continue #Not in use
				out.append(entry[2])
		return out

#**************** File class *******************

class VsfsFile(object):
	def __init__(self, fileInode, parent, mode):
		self.parent = parent
		self.fileInode = fileInode
		self.mode = mode
		self.meta, self.dataPtrs = self.parent._load_inode(fileInode)
		self.cursor = 0
		self.closed = False
		self.backfillWithZeros = True
		if self.mode != "r" and self.mode != "w":
			raise ValueError("Only r and w modes implemented")

		self.usedDataPtrs = []
		for ptr in self.dataPtrs:
			if ptr is not None:
				self.usedDataPtrs.append(ptr)

	def __len__(self):
		return self.meta['fileSize']

	def write(self, data):
		if self.closed:
			raise IOError("File already closed")
		if self.mode != "w":
			raise RuntimeError("Not in write mode")
		if self.cursor >= self.parent.maxFileSize:
			raise IOError("Cursor beyond maximum file size")

		writing = True
		currentData = bytearray(data)
		bytesWritten = 0

		#Check if we need to backfill file up to cursor
		if self.cursor > self.meta['fileSize'] and self.backfillWithZeros:
			extraZerosSize = self.cursor - self.meta['fileSize']
			zeros = bytearray("".join(['\x00' for i in range(extraZerosSize)]))
			currentData = zeros + currentData
			self.cursor = self.meta['fileSize']

		while writing:			
			inBlockNum = self.cursor / self.parent.blockSize
			if inBlockNum < len(self.usedDataPtrs):
				#Use already allocated data blocks
				posInBlock = self.cursor % self.parent.blockSize
				bytesToBlockEnd = self.parent.blockSize - posInBlock
				bytesToMaxFileSize = self.parent.maxFileSize - self.cursor					

				bytesToWrite = bytesToBlockEnd
				if bytesToWrite > len(currentData):
					bytesToWrite = len(currentData)
				if bytesToWrite > bytesToMaxFileSize:
					bytesToWrite = bytesToMaxFileSize

				datToWrite = currentData[:bytesToWrite]
				self.parent._write_to_data_block(self.usedDataPtrs[inBlockNum], posInBlock, datToWrite)
				bytesWritten += len(datToWrite)
				currentData = currentData[len(datToWrite):]

				self.cursor += len(datToWrite)

				if self.cursor > self.meta['fileSize']:
					#print "file mini-expand"
					self.meta['fileSize'] = self.cursor

				if len(currentData) == 0:
					writing = False

				if self.cursor >= self.parent.maxFileSize:
					self.parent._update_file_size(self.fileInode, self.meta['fileSize'])
					raise IOError("Maximum file size reached")

			else:
				#Allocate more data blocks
				if len(self.usedDataPtrs) == len(self.dataPtrs):
					raise RuntimeError("Inode pointer table full, file already max size")

				requiredBlocks = int(math.ceil(float(len(currentData)) / self.parent.blockSize))
				allocatedBlocks = self.parent._allocate_space_to_inode(self.fileInode, requiredBlocks)
				self.usedDataPtrs.extend(allocatedBlocks)

				continue

			if len(currentData) == 0:
				writing = False

		#Update file size in inode table
		self.parent._update_file_size(self.fileInode, self.meta['fileSize'])

		return bytesWritten

	def read(self, readLen):
		if self.closed:
			raise IOError("File already closed")
		if self.mode != "r":
			raise RuntimeError("Not in read mode")

		numBlocksAlloc = len(self.dataPtrs)
		reading = True
		outBuff = []

		while reading:			
			inBlockNum = self.cursor / self.parent.blockSize
			if inBlockNum < numBlocksAlloc and self.cursor <= self.meta['fileSize'] and self.cursor < self.meta['fileSize']:
				#Use already allocated data blocks
				posInBlock = self.cursor % self.parent.blockSize
				bytesToBlockEnd = self.parent.blockSize - posInBlock
				bytesToFileEnd = self.meta['fileSize'] - self.cursor
				bytesToRead = bytesToBlockEnd
				#print bytesToBlockEnd, readLen, bytesToFileEnd, self.cursor, self.meta['fileSize']
				if readLen < bytesToRead:
					bytesToRead = readLen
				if bytesToRead > bytesToFileEnd:
					 bytesToRead = bytesToFileEnd

				tmpDat = self.parent._read_from_data_block(self.dataPtrs[inBlockNum], posInBlock, bytesToRead)

				readLen -= len(tmpDat)
				outBuff.append(tmpDat)
			else:
				#End of file
				reading = False

			if readLen <= 0:
				reading = False

		return "".join(outBuff)

	def seek(self, pos, seekFrom = 0):
		if seekFrom == 0:
			if pos < 0:
				raise IOError("Cannot seek to negative position")
			self.cursor = pos
		else:
			raise RuntimeError("Not implemented")

	def tell(self):
		return self.cursor

	def close(self):
		self.closed = True

	def flush(self):
		self.parent.flush()
	
