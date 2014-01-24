
import struct, json, os, random, string, hashlib

class HashTableFile(object):
	def __init__(self, fi):
		createFile = False
		if isinstance(fi, str):
			createFile = not os.path.isfile(fi)
			if createFile:
				self.handle = open(fi, "w+b")
			else:
				self.handle = open(fi, "r+b")
		else:
			self.handle = fi

		self.headerReservedSpace = 64
		self.hashHeaderStruct = struct.Struct(">IQ") #Hash bit length size, items
		self.binStruct = struct.Struct(">BQQQ") #In use, hash, key, value
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
		self.handle.write(self.hashHeaderStruct.pack(self.hashMaskSize, self.numItems))
		self.handle.flush()

	def _init_storage(self):
		print "_init_storage"
		self.handle.seek(0)
		self.handle.write("hash")
		self.numItems = 0
		self.handle.write(self.hashHeaderStruct.pack(self.hashMaskSize, self.numItems))

		self.labelStart = self.hashMask * self.binStruct.size + self.headerReservedSpace
		self.handle.seek(self.labelStart)
		for count in range(self.labelReservedSpace):
			self.handle.write("\x00")

		self.handle.seek(self.labelStart)
		self.handle.write("labl")

		self.handle.flush()

	def _read_storage_params(self):
		print "_read_storage_params"
		self.handle.seek(0)
		if self.handle.read(4) != "hash":
			raise Exception("Unknown file format")

		header = self.handle.read(self.hashHeaderStruct.size)
		self.hashMaskSize, self.numItems = self.hashHeaderStruct.unpack(header)
		self.hashMask = pow(2, self.hashMaskSize)

	def __getitem__(self, k):

		primaryKeyHash = self._hash_label(k) % self.hashMask
		keyHash = primaryKeyHash
		found = 0
		while not found:
			ret, val = self._attempt_to_read_bin(keyHash, k)
			if ret == 1:
				return val
			if ret == -1:
				raise Exception("Not key found")

			keyHash += 1
			keyHash = keyHash % self.hashMask	

			if keyHash == primaryKeyHash:
				raise Exception("Not key found")

	def __len__(self):
		return self.numItems

	def __setitem__(self, k, v):

		primaryKeyHash = self._hash_label(k) % self.hashMask
		keyHash = primaryKeyHash
		done = 0
		while not done:
			done = self._attempt_to_write_bin(keyHash, k, v)
			if not done:
				keyHash += 1
				keyHash = keyHash % self.hashMask
				if keyHash == primaryKeyHash:
					raise Exception("Hash table full")

	def _attempt_to_read_bin(self, keyHash, k):
		binFiOffset = keyHash * self.binStruct.size + self.headerReservedSpace
		#print "binFiOffset", binFiOffset
		self.handle.seek(binFiOffset)
		tmp = self.handle.read(self.binStruct.size)
		inUse, existingHash, existingKey, existingVal = self.binStruct.unpack(tmp)		
		#print inUse, existingHash, existingKey, existingVal, self._get_label(existingKey), keyHash

		if not inUse:
			return -1, None #Empty bin

		if existingHash != keyHash:
			return 0, None #No match

		oldKey = self._get_label(existingKey)
		if oldKey != self._mash_label(k):
			return 0, None #No match

		#Match
		return 1, self._get_label(existingVal)

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
		inUse, existingHash, existingKey, existingVal = self.binStruct.unpack(tmp)
		#print "inUse", inUse

		if not inUse:
			#Write to primary bin
			klo = self._write_label(k)
			vlo = self._write_label(v)

			self.handle.seek(binFiOffset)
			binData = self.binStruct.pack(1, keyHash, klo, vlo)
			self.handle.write(binData)
			self.handle.flush()

			self.numItems += 1
			if self.verbose: print "data inserted"
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
						if self.verbose: print "value updated"
						return 1
					else:
						if self.verbose: print "value unchanged"
						return 1 #No change was made to value

			#Hash collision
			#Find an alternative bin
		return 0

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
			return self.handle.read(textLen).decode("utf-8")

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
		pass	

	def _mark_label_unused(self, pos):
		self.handle.seek(pos)
		self.handle.write('\x00')

def RandomObj():
	ty = random.randint(0, 2)
	if ty == 0:
		return random.randint(0, 1000)
	if ty == 1:
		N = 10
		return ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(N))
	return (RandomObj(), RandomObj())

if __name__ == "__main__":
	os.unlink("table.hash")
	table = HashTableFile("table.hash")

	test = dict()
	for i in range(5):
		test[RandomObj()] = RandomObj()

	for k in test:
		print "Set", k, "=" , test[k]
		table[k] = test[k]

	for k in test:
		print k, "is", table[k], ", expected", test[k]


