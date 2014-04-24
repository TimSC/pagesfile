
import struct, json, os, random, string, hashlib, math, pickle

class HashTableFile(object):
	def __init__(self, fi, maskBits = 3, 
		init_storage=False, modulusIntHash = 0, hashGradient = 5, hashOffset = 1, readOnly = False, createFile = False):
		
		"""
		A hash table using open addressing.
		Each slot in the table can store one and only one entry.
		Each slot contains a hash, key and value
		Labels are encoded into the slot or are stored at the end of the file as a label
		Random probing is used if there as a collision
		The table will be resized if it is two-thirds full
		Implementation is largely inspired by the CPython dict implementation
		https://stackoverflow.com/questions/327311/how-are-pythons-built-in-dictionaries-implemented
		"""
		self.debugMode = False
		self.readOnly = readOnly
		if isinstance(fi, str):
			if not createFile:
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

		self.headerReservedSpace = 64
		self.hashHeaderStruct = struct.Struct(">IQQBII") #Hash bit length size, num items, num bins used
		if self.headerReservedSpace < self.hashHeaderStruct.size:
			raise RuntimeError("Header struct too big for allocated size")

		self.labelReservedSpace = 64
		self.verbose = 0
		self.usePickle = 1 #otherwise use json

		#Hash preferences
		self.modulusIntHash = modulusIntHash
		self.hashGradient = hashGradient
		self.hashOffset = hashOffset

		if createFile or init_storage:
			if self.readOnly:
				raise Exception("Cannot format hash structure in read only mode.")

			self.hashMaskSize = maskBits
			self.hashMask = pow(2, self.hashMaskSize)
			self._init_storage()
		else:
			self._read_storage_params()

	def __del__(self):
		print "hash table flush"
		self.flush()

	def clear(self):
		if self.readOnly:
			raise Exception("Cannot clear table in read only mode.")

		#Clear hash table
		for binNum in xrange(self.hashMask):
			if binNum % 100000 == 0 and self.verbose >= 2:
				print binNum, self.hashMask

			binFiOffset = binNum * self.binStruct.size + self.headerReservedSpace
			self.handle.seek(binFiOffset)
			binData = self.binStruct.pack(0x00, 0, 0, 0)
			self.handle.write(binData)
			#self.handle.flush() #Massive performance hit

		self.numItems = 0
		self.binsInUse = 0

		#Clear labels
		labelDataStart = self.hashMask * self.binStruct.size + self.headerReservedSpace + self.labelReservedSpace
		cursor = labelDataStart		
		while True:
			self.handle.seek(cursor)
			labelHeader = self.handle.read(5)
			if self.verbose >= 2:
				print "cursor", cursor, len(labelHeader), len(self.handle)
			if len(labelHeader) < 5: break
			labelType, labelLen = struct.unpack(">BI", labelHeader)

			#Set to unused
			if labelType != 0x00:
				self._mark_label_unused(cursor)
			
			cursor += 9 + labelLen

	def flush(self):
		if self.readOnly:
			return

		self.handle.seek(4)
		self.handle.write(self.hashHeaderStruct.pack(self.hashMaskSize, self.numItems, self.binsInUse,
			self.modulusIntHash, self.hashGradient, self.hashOffset))
		self.handle.flush()

	def _set_bin_struct(self):
		if self.hashMaskSize <= 8:
			self.binStruct = struct.Struct(">BBQQ") #Flags, hash, key, value
		elif self.hashMaskSize <= 16:
			self.binStruct = struct.Struct(">BHQQ") #Flags, hash, key, value
		elif self.hashMaskSize <= 32:
			self.binStruct = struct.Struct(">BIQQ") #Flags, hash, key, value
		else:
			self.binStruct = struct.Struct(">BQQQ") #Flags, hash, key, value

	def _init_storage(self):
		if self.verbose: print "_init_storage"
		self.handle.seek(0)
		self.handle.write("hash")
		self.numItems = 0
		self.binsInUse = 0
		self.handle.write(self.hashHeaderStruct.pack(self.hashMaskSize, self.numItems, self.binsInUse,
			self.modulusIntHash, self.hashGradient, self.hashOffset))
		self._set_bin_struct()

		self.labelStart = self.hashMask * self.binStruct.size + self.headerReservedSpace
		self.handle.seek(self.labelStart)
		for count in range(self.labelReservedSpace):
			self.handle.write("\x00")

		self.handle.seek(self.labelStart)
		self.handle.write("labl")

		self.handle.flush()

	def _read_storage_params(self):
		if self.verbose: print "_read_storage_params"
		self.handle.seek(0)
		if self.handle.read(4) != "hash":
			raise Exception("Unknown file format")

		header = self.handle.read(self.hashHeaderStruct.size)
		self.hashMaskSize, self.numItems, self.binsInUse, \
			self.modulusIntHash, self.hashGradient, self.hashOffset = self.hashHeaderStruct.unpack(header)
		self.hashMask = pow(2, self.hashMaskSize)
		self._set_bin_struct()

	def _probe_bins(self, k):

		#Use simple modules if this mode is enabled
		if self.modulusIntHash and isinstance(k, int):
			primaryKeyHash = k % self.hashMask
		else:
			primaryKeyHash = self._hash_label(k) % self.hashMask
		keyHash = primaryKeyHash
		found = 0
		trashHashes = []
		probeCount = 0

		#print "primary key", primaryKeyHash
		while not found:
			#print "look in bin", keyHash
			try:
				ret, flags, key, val = self._attempt_to_read_bin(keyHash, k, False)
			except Exception as err:
				if self.debugMode:
					print "keyHash", keyHash
					print "k", k, type(k)
					print "self.hashMask", self.hashMask
					print "probeCount", probeCount
					print "self.modulusIntHash", self.modulusIntHash
				raise RuntimeError(err)
			probeCount += 1
			inUse = flags & 0x01
			inTrash = flags & 0x02

			if ret == 1:
				return 1, key, val, trashHashes, keyHash
			if ret == -1:
				return -1, None, None, trashHashes, keyHash
			if ret == 0 and inTrash:
				trashHashes.append(keyHash)

			keyHash = ((self.hashGradient*keyHash) + self.hashOffset) % self.hashMask

			if keyHash == primaryKeyHash:
				#Searched entire table, still not found
				return -2, None, None, trashHashes, None

			#if probeCount > 10:
			#	print "warning: probe count", probeCount, keyHash, self.hashMask

	def __getitem__(self, k):
		ret, key, val, trashHashes, actualBin = self._probe_bins(k)
		if ret == 1:
			return val
		raise IndexError("Key not found")

	def __len__(self):
		return self.numItems

	def __contains__(self, k):
		ret, key, val, trashHashes, actualBin = self._probe_bins(k)
		return ret == 1

	def __setitem__(self, k, v):

		if self.readOnly:
			raise Exception("Cannot set item when table is in read only mode.")

		#print "setitem", k , type(k),"=", v
		ret, key, val, trashHashes, actualBin = self._probe_bins(k)

		if ret == 1:
			#Update existing entry
			self._attempt_to_write_bin(actualBin, k, v)

		if ret == -1 and len(trashHashes) > 0:
			#Use trash location for data
			done = self._attempt_to_write_bin(trashHashes[0], k, v)

		if ret == -1 and len(trashHashes) == 0:
			#Use new location for data
			self._attempt_to_write_bin(actualBin, k, v)
		
		if ret == -2:
			raise Exception("Hash table full")

		#Check if we need to resize
		if self.binsInUse > self.hashMask * 2. / 3. and self.haveFileOwnership:
			#Increase mask size by two bits
			self.allocate_mask_size(self.hashMaskSize + 2)

	def _attempt_to_read_bin(self, keyHash, k, matchAny = False):
		if keyHash >= self.hashMask:
			raise IndexError("Invalid bin")

		binFiOffset = keyHash * self.binStruct.size + self.headerReservedSpace
		#print "binFiOffset", binFiOffset
		self.handle.seek(binFiOffset)
		tmp = self.handle.read(self.binStruct.size)
		flags, existingHash, existingKey, existingVal = self.binStruct.unpack(tmp)		
		#print inUse, existingHash, existingKey, existingVal, self._get_label(existingKey), keyHash
		inUse = flags & 0x01
		inTrash = flags & 0x02
		existingRawKey = flags & 0x04
		existingRawValue = flags & 0x08
		#print keyHash, "flagsA", flags
		#if inUse and not existingRawKey: print keyHash, "binkey", self._get_label(existingKey)
		#if inUse and existingRawKey: print keyHash, "binkey", existingKey

		if not inUse:
			return -1, flags, None, None #Empty bin

		if inTrash:
			return 0, flags, None, None #Trashed bin

		if not matchAny and existingHash != keyHash:
			return 0, flags, None, None #No match

		if existingRawKey:
			oldKey = existingKey
		else:
			try:
				oldKey = self._get_label(existingKey)
			except:
				if self.debugMode:
					print "flags", flags
					print "existingHash", existingHash
					print "existingRawKey", existingRawKey
					print "existingRawValue", existingRawValue
				raise RuntimeError("Failed to retrieve key data for:" + str(existingKey))

		if not matchAny and oldKey != self._mash_label(k):
			return 0, flags, None, None #No match

		#Match
		if existingRawValue:
			val = existingVal
		else:
			val = self._get_label(existingVal)

		return 1, flags, oldKey, val

	def _mash_label(self, label):
		#Encoding a label can make subtle changes
		#We need to emulate this change in a particular case
		if isinstance(label, str):
			return label

		if self.usePickle:
			return pickle.loads(pickle.dumps(label, protocol = -1))
		return json.loads(json.dumps(label))

	def _attempt_to_write_bin(self, keyHash, k, v):
		binFiOffset = keyHash * self.binStruct.size + self.headerReservedSpace
		#print "binFiOffset", binFiOffset
		self.handle.seek(binFiOffset)
		tmp = self.handle.read(self.binStruct.size)
		flags, existingHash, existingKey, existingVal = self.binStruct.unpack(tmp)
		inUse = flags & 0x01
		inTrash = flags & 0x02
		existingRawKey = flags & 0x04
		existingRawValue = flags & 0x08
		#print "inUse", inUse

		if not inUse or inTrash:
			newFlags = 0x01 #In use
			if isinstance(k, int):
				klo = k
				newFlags = newFlags | 0x04 #Key is a raw int
			else:
				klo = self._write_label(k)
			
			if isinstance(v, int):
				vlo = v
				newFlags = newFlags | 0x08 #value is a raw int
			else:
				vlo = self._write_label(v)

			self.handle.seek(binFiOffset)
			binData = self.binStruct.pack(newFlags, keyHash, klo, vlo)
			self.handle.write(binData)

			self.numItems += 1
			self.binsInUse += 1
			if self.verbose >= 2: print "data inserted"
			return 1

		else:
			#Check if item already exists
			newFlags = 0x01 #In use

			if keyHash != existingHash:
				#Key does not match expected value
				raise Exception("Internal error writing to bin")

			if existingRawKey:
				oldKey = existingKey
				newFlags = newFlags | 0x04 #key is a raw int
			else:
				oldKey = self._get_label(existingKey)

			if oldKey != k:
				#Key does not match expected value
				raise Exception("Internal error writing to bin: unmatched keys")

			if existingRawValue:
				oldValue = existingVal
			else:
				oldValue = self._get_label(existingVal)

			if oldValue != v:
					
				if isinstance(v, int):
					vlo = v
					newFlags = newFlags | 0x08 #value is a raw int
				else:
					vlo = self._write_label(v)

				if not existingRawValue:
					self._mark_label_unused(existingVal)

				self.handle.seek(binFiOffset)
				binData = self.binStruct.pack(newFlags, keyHash, existingKey, vlo)
				self.handle.write(binData)
				#self.handle.flush() #Massive performance hit
				if self.verbose >= 2: print "value updated"
				return 1
			else:
				if self.verbose >= 2: print "value unchanged"
				return 1 #No change was made to value

	def _write_label(self, label):
		self.handle.seek(0,2) #Seek to end
		pos = self.handle.tell()

		if isinstance(label, str):
			#UTF-8 string
			strenc = label.encode('utf-8')
			labelLen = struct.pack(">I", len(strenc))
			self.handle.write('\x01')
			self.handle.write(labelLen)
			self.handle.write(strenc)
			self.handle.write(labelLen)
			return pos
		
		#Pickle or JSON is used as the fallback encoder
		if self.usePickle:
			enc = pickle.dumps(label, protocol = -1)
			labelLen = struct.pack(">I", len(enc))
			self.handle.write('\x03')
			self.handle.write(labelLen)
			self.handle.write(enc)
			self.handle.write(labelLen)			
		else:
			enc = json.dumps(label)
			labelLen = struct.pack(">I", len(enc))
			self.handle.write('\x02')
			self.handle.write(labelLen)
			self.handle.write(enc)
			self.handle.write(labelLen)
		return pos

	def _get_label(self, pos):
		self.handle.seek(pos)
		rawType = self.handle.read(1)
		if len(rawType)==0:
			raise RuntimeError("Error reading label type")
		labelType = ord(rawType)

		if labelType == 0x01:
			#UTF-8 string
			lenBin = self.handle.read(4)
			textLen = struct.unpack(">I", lenBin)[0]
			return str(self.handle.read(textLen).decode("utf-8"))

		if labelType == 0x02:
			#JSON data
			lenBin = self.handle.read(4)
			textLen = struct.unpack(">I", lenBin)[0]
			jsonDat = self.handle.read(textLen)
			return json.loads(jsonDat)

		if labelType == 0x03:
			#Pickle data
			lenBin = self.handle.read(4)
			textLen = struct.unpack(">I", lenBin)[0]
			jsonDat = self.handle.read(textLen)
			return pickle.loads(jsonDat)

		raise Exception("Unsupported data type: "+str(labelType))
		
	def _hash(self, data):
		h = hashlib.md5(data).digest()
		return struct.unpack("=Q", h[:8])[0]
		
	def _hash_label(self, label):
		label = self._mash_label(label)

		if isinstance(label, str):
			strenc = label.encode('utf-8')
			return self._hash(strenc)

		if self.usePickle:
			enc = pickle.dumps(label)
			return self._hash(enc)
		else:
			enc = json.dumps(label)
			return self._hash(enc)

	def _get_label_hash(self, pos):
		self.handle.seek(pos)
		labelType = ord(self.handle.read(1))

		if labelType == 0x01: #UTF-8 string
			lenBin = self.handle.read(4)
			textLen = struct.unpack(">I", lenBin)[0]
			txt = self.handle.read(textLen)
			return self._hash(txt)

		if labelType == 0x02: #JSON data
			lenBin = self.handle.read(4)
			textLen = struct.unpack(">I", lenBin)[0]
			txt = self.handle.read(textLen)
			return self._hash(txt)

		if labelType == 0x03: #Pickle data
			lenBin = self.handle.read(4)
			textLen = struct.unpack(">I", lenBin)[0]
			txt = self.handle.read(textLen)
			return self._hash(txt)

		raise Exception("Unsupported data type: "+str(labelType))

	def __delitem__(self, k):

		ret, key, val, trashHashes, actualBin = self._probe_bins(k)

		if ret == 1:
			#Found bin, now set trash flag
			binFiOffset = actualBin * self.binStruct.size + self.headerReservedSpace
			self.handle.seek(binFiOffset)
			tmp = self.handle.read(self.binStruct.size)
			flags, existingHash, existingKey, existingVal = self.binStruct.unpack(tmp)
			existingRawKey = flags & 0x04
			existingRawValue = flags & 0x08

			flags = flags | 0x02
			self.handle.seek(binFiOffset)
			newBinVals = self.binStruct.pack(flags, 0, 0, 0)
			self.handle.write(newBinVals)

			if not existingRawKey:
				self._mark_label_unused(existingKey)
			if not existingRawValue:
				self._mark_label_unused(existingVal)
			self.numItems -= 1
				
			return


	def __iter__(self):
		return HashTableFileIter(self)

	def _mark_label_unused(self, pos):
		self.handle.seek(pos)
		self.handle.write('\x00')

	def allocate_mask_size_owned(self, maskBits):
		#This uses a file rename to skip copying the data
		if self.verbose: print "allocate_mask_size", maskBits

		if maskBits > 64:
			raise Exception("Maximum hash length is 64 bits")
	
		#Copy table to temp file
		self.flush()
		self.handle.close()
		tmpFilename = self.filename + str(random.randint(0,1000000)) + ".tmp"
		os.rename(self.filename, tmpFilename)
		oldTable = HashTableFile(tmpFilename)

		#Recreate table
		self.hashMaskSize = maskBits
		self.hashMask = pow(2, self.hashMaskSize)
		self.handle = open(self.filename, "w+b")
		self._init_storage()

		#Copy data from old table
		#print "old length", len(oldTable)
		for k in oldTable:
			#print "copying", k
			self.__setitem__(k, oldTable[k])
		self.flush()

		#Delete old table temp file
		del oldTable
		try:
			os.unlink(tmpFilename)
		except:
			pass

	def allocate_mask_size(self, maskBits):
		if self.haveFileOwnership:
			self.allocate_mask_size_owned(maskBits)
		else:
			raise Exception("This cannot be done. Use allocate_mask_size_safe function instead")

	def allocate_size(self, dataSize):
		requiredBits = int(math.ceil(math.log(dataSize * 3. / 2., 2)))
		self.allocate_mask_size(requiredBits)

class HashTableFileIter(object):
	def __init__(self, parent):
		self.parent = parent
		self.nextBinNum = 0

	def __iter__(self):
		return self

	def next(self):
		while True:
			if self.nextBinNum >= self.parent.hashMask:
				raise StopIteration()

			found, flags, key, val = self.parent._attempt_to_read_bin(self.nextBinNum, None, True)
			self.nextBinNum += 1

			if found == 1:
				#print "Iterator pos", self.nextBinNum - 1
				return key

# **********************************************************************

def allocate_mask_size_safe(filename, maskBits):
	#Resizing should not be done on an opened file
	if maskBits > 64:
		raise Exception("Maximum hash length is 64 bits")
	
	#Copy table to temp file
	tmpFilename = filename + str(random.randint(0,1000000)) + ".tmp"
	os.rename(filename, tmpFilename)
	oldTable = HashTableFile(tmpFilename)

	#Recreate table
	newTable = HashTableFile(filename, maskBits)

	#Copy data from old table
	#print "old length", len(oldTable)
	for k in oldTable:
		#print "copying", k
		newTable.__setitem__(k, oldTable[k])
	newTable.flush()

	#Delete old table temp file
	del oldTable
	try:
		os.unlink(tmpFilename)
	except:
		pass

def RandomObj():
	ty = random.randint(0, 2)
	if ty == 0:
		return random.randint(0, 1000)
	if ty == 1:
		N = 10
		return ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(N))
	return (RandomObj(), RandomObj())

if __name__ == "__main__":
	try:
		os.unlink("table.hash")
	except:
		pass
	table = HashTableFile("table.hash")
	table.verbose = 1
	
	test = dict()
	table.allocate_size(5)
	for i in range(5):
		test[RandomObj()] = RandomObj()

	for i, k in enumerate(test):
		print i, "Set", k, "=" , test[k]
		if len(table) != i:
			print "Unexpected table size", len(table), i
		table[k] = test[k]

	for k in test:
		print k, "is", table[k], ", expected", test[k]

	print "Num items", len(table), "expected", len(test)

	if 0:
		#Delete random value
		for i in range(5):
			randKey = random.choice(test.keys())
			print "Delete key", randKey

			del test[randKey]
			del table[randKey]

			k = RandomObj()
			v = RandomObj()
			table[k] = v
			test[k] = v
			print "Set", k, "=" , test[k]
		
			print "Read back", k, "=", table[k]

		for i, k in enumerate(table):
			v = table[k]
			print i, k, v

		print "Num items", len(table), "expected", len(test)

	table.clear()
	print "Clear again"
	table.clear()
	#table.allocate_mask_size(10)

	del table
	allocate_mask_size_safe("table.hash", 16)
	table = HashTableFile("table.hash")
	print table.hashMaskSize

