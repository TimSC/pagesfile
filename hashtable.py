
import struct, json, os, random, string, hashlib

class HashTableFile(object):
	def __init__(self, fi):
		createFile = not os.path.isfile(fi)
		self.filename = fi
		if createFile:
			self.handle = open(fi, "w+b")
		else:
			self.handle = open(fi, "r+b")

		self.headerReservedSpace = 64
		self.hashHeaderStruct = struct.Struct(">IQQ") #Hash bit length size, num items, num bins used
		self.labelReservedSpace = 64
		self.verbose = 0

		if createFile:
			self.hashMaskSize = 3
			self.hashMask = pow(2, self.hashMaskSize)
			self._init_storage()

		else:
			self._read_storage_params()

	def __del__(self):
		self.flush()

	def flush(self):
		self.handle.seek(4)
		self.handle.write(self.hashHeaderStruct.pack(self.hashMaskSize, self.numItems, self.binsInUse))
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
		self.handle.write(self.hashHeaderStruct.pack(self.hashMaskSize, self.numItems, self.binsInUse))
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
		self.hashMaskSize, self.numItems, self.binsInUse = self.hashHeaderStruct.unpack(header)
		self.hashMask = pow(2, self.hashMaskSize)
		self._set_bin_struct()

	def _probe_bins(self, k):
		primaryKeyHash = self._hash_label(k) % self.hashMask
		keyHash = primaryKeyHash
		found = 0
		trashHashes = []
		#print "primary key", primaryKeyHash
		while not found:
			ret, flags, key, val = self._attempt_to_read_bin(keyHash, k, False)
			inUse = flags & 0x01
			inTrash = flags & 0x02			

			if ret == 1:
				return 1, key, val, trashHashes, keyHash
			if ret == -1:
				return -1, None, None, trashHashes, keyHash
			if ret == 0 and inTrash:
				trashHashes.append(keyHash)

			keyHash = ((5*keyHash) + 1) % self.hashMask

			if keyHash == primaryKeyHash:
				#Searched entire table, still not found
				return -2, None, None, trashHashes, None

	def __getitem__(self, k):
		ret, key, val, trashHashes, actualBin = self._probe_bins(k)
		if ret == 1:
			return val
		raise IndexError("Key not found")

	def __len__(self):
		return self.numItems

	def __setitem__(self, k, v):

		#print "setitem", k , type(k),"=", v
		ret, key, val, trashHashes, actualBin = self._probe_bins(k)

		if ret == 1:
			#Update existing entry
			self._attempt_to_write_bin(actualBin, k, v)

		if ret == -1 and len(trashHashes) > 0:
			#Use trash location for data
			print "YAY"
			done = self._attempt_to_write_bin(trashHashes[0], k, v)

		if ret == -1 and len(trashHashes) == 0:
			#Use new location for data
			self._attempt_to_write_bin(actualBin, k, v)
		
		if ret == -2:
			raise Exception("Hash table full")

		#Check if we need to resize
		if self.binsInUse > self.hashMask * 2. / 3.:
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
		#print keyHash, "flagsA", flags
		#if inUse: print keyHash, "binkey", self._get_label(existingKey)

		if not inUse:
			return -1, flags, None, None #Empty bin

		if inTrash:
			return 0, flags, None, None #Trashed bin

		if not matchAny and existingHash != keyHash:
			return 0, flags, None, None #No match

		oldKey = self._get_label(existingKey)
		if not matchAny and oldKey != self._mash_label(k):
			return 0, flags, None, None #No match

		#Match
		return 1, flags, oldKey, self._get_label(existingVal)

	def _mash_label(self, label):
		#Encoding a label can make subtle changes
		#We need to emulate this change in a particular case
		if isinstance(label, str):
			return label

		return json.loads(json.dumps(label))

	def _attempt_to_write_bin(self, keyHash, k, v):
		binFiOffset = keyHash * self.binStruct.size + self.headerReservedSpace
		#print "binFiOffset", binFiOffset
		self.handle.seek(binFiOffset)
		tmp = self.handle.read(self.binStruct.size)
		flags, existingHash, existingKey, existingVal = self.binStruct.unpack(tmp)
		inUse = flags & 0x01
		inTrash = flags & 0x02
		#print "inUse", inUse

		if not inUse or inTrash:
			klo = self._write_label(k)
			vlo = self._write_label(v)

			self.handle.seek(binFiOffset)
			binData = self.binStruct.pack(1, keyHash, klo, vlo)
			self.handle.write(binData)
			self.handle.flush()

			self.numItems += 1
			self.binsInUse += 1
			if self.verbose >= 2: print "data inserted"
			return 1

		else:
			#Check if item already exists
			if keyHash == existingHash:
				oldKey = self._get_label(existingKey)
				if oldKey == k:
					oldValue = self._get_label(existingVal)
					if oldValue != v:
						vlo = self._write_label(v)
						self._mark_label_unused(existingVal)

						self.handle.seek(binFiOffset)
						binData = self.binStruct.pack(1, keyHash, existingKey, vlo)
						self.handle.write(binData)
						self.handle.flush()
						if self.verbose >= 2: print "value updated"
						return 1
					else:
						if self.verbose >= 2: print "value unchanged"
						return 1 #No change was made to value

			#Key does not match expected value
			raise Exception("Internal error writing to bin")

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
		
		#JSON is fallback encoder
		enc = json.dumps(label)
		labelLen = struct.pack(">I", len(enc))
		self.handle.write('\x02')
		self.handle.write(labelLen)
		self.handle.write(enc)
		self.handle.write(labelLen)
		return pos

	def _get_label(self, pos):
		self.handle.seek(pos)
		labelType = ord(self.handle.read(1))

		if labelType == 1:
			#UTF-8 string
			lenBin = self.handle.read(4)
			textLen = struct.unpack(">I", lenBin)[0]
			return str(self.handle.read(textLen).decode("utf-8"))

		if labelType == 2:
			#JSON data
			lenBin = self.handle.read(4)
			textLen = struct.unpack(">I", lenBin)[0]
			jsonDat = self.handle.read(textLen)
			return json.loads(jsonDat)

		raise Exception("Unsupported data type: "+str(labelType))
		
	def _hash(self, data):
		h = hashlib.md5(data).digest()
		return struct.unpack("=Q", h[:8])[0]
		
	def _hash_label(self, label):
		label = self._mash_label(label)

		if isinstance(label, str):
			strenc = label.encode('utf-8')
			return self._hash(strenc)

		enc = json.dumps(label)
		return self._hash(enc)

	def _get_label_hash(self, pos):
		self.handle.seek(pos)
		labelType = ord(self.handle.read(1))

		if labelType == 1: #UTF-8 string
			lenBin = self.handle.read(4)
			textLen = struct.unpack(">I", lenBin)[0]
			txt = self.handle.read(textLen)
			return self._hash(txt)

		if labelType == 2: #JSON data
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

			flags = flags | 0x02
			self.handle.seek(binFiOffset)
			newBinVals = self.binStruct.pack(flags, 0, 0, 0)
			self.handle.write(newBinVals)

			self._mark_label_unused(existingKey)
			self._mark_label_unused(existingVal)
			self.numItems -= 1
				
			return


	def __iter__(self):
		return HashTableFileIter(self)

	def _mark_label_unused(self, pos):
		self.handle.seek(pos)
		self.handle.write('\x00')

	def allocate_mask_size(self, maskBits):
		if self.verbose: print "allocate_mask_size", maskBits

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
	for i in range(100000):
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

	#table.allocate_mask_size(10)

