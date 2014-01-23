import bz2, struct

class PagesFile(object):
	def __init__(self, fi, mode = "r"):
		if isinstance(fi, str):
			self.handle = open(fi, mode+"b")
		else:
			self.handle = fi
		self.mode = mode
		self.maxPlainSize = 1000000
		self.buffer = None
		self.method = "bz2"
		self.writtenPlainBytes = 0
		self.virtualCursor = 0
		self.cursorPagePlain = None
		self.pageIndex = None
		self.cursorPageMeta = None
		self.plainLen = None
		self.headerStruct = struct.Struct(">QQQ")
		self.footerStruct = struct.Struct(">Q")
		
		if self.mode == "w":
			self.handle.write("pset")

	def __del__(self):

		if self.mode == "w":
			self.flush()
			self.handle.write("fini")
			self.handle.flush()

	def flush(self):
		if self.buffer is not None:
			self._write_page(self.buffer)
		self.buffer = None

	def write(self, data):
		if self.mode != "w":
			raise Exception("Wrong file mode, cannot write")

		if self.buffer is None:
			self.buffer = data[:] #Get a local copy
		else:
			self.buffer += data

		if len(self.buffer) > self.maxPlainSize:
			page = self.buffer[:self.maxPlainSize]
			self.buffer = self.buffer[self.maxPlainSize:]
			self._write_page(page)

	def _refresh_page_index(self):
		self.handle.seek(0)
		if self.handle.read(4) != "pset":
			raise Exception("File format not recognised")

		self.pageIndex = []
		while True:			
			meta = self._parse_header_at_cursor()
			if meta is None:
				break
			print meta
			self.pageIndex.append(meta)
			pos = self.handle.tell()
			self.handle.seek(pos + meta[1])
			footerData = self.handle.read(8)
			endStr = self.handle.read(4)
			if endStr != "pend":
				raise Exception("File format not recognised")

	def _parse_header_at_cursor(self):
		pagePos = self.handle.tell()
		startStr = self.handle.read(4)
		if startStr == "fini":
			return None
		if startStr != "page":
			raise Exception("File format not recognised")

		header = self.handle.read(self.headerStruct.size)
		uncompSize, compSize, uncompPos = self.headerStruct.unpack(header)
		method = self.handle.read(4)
		return pagePos, compSize, uncompPos, uncompSize, method

	def read(self, bytes):
		if self.mode != "r":
			raise Exception("Wrong file mode, cannot read")

		if self.pageIndex is None:
			self._refresh_page_index()

		#Check if current page is suitable
		if self.cursorPageMeta is not None and \
			self.virtualCursor >= self.cursorPageMeta[2] and \
			self.virtualCursor < self.cursorPageMeta[2] + self.cursorPageMeta[3]:

			localCursor = self.virtualCursor - self.cursorPageMeta[2]
			retStr = self.cursorPagePlain[localCursor: localCursor+bytes]
			self.virtualCursor += len(retStr)
			return retStr


		#Seek for suitable page
		self.cursorPageMeta = None
		self.cursorPagePlain = None
		for page in self.pageIndex:
			if self.virtualCursor >= page[2] and self.virtualCursor < page[2] + page[3]:
				self.cursorPageMeta = page
				self.cursorPagePlain = None

		if self.cursorPageMeta is None:
			return ""

		self.handle.seek(self.cursorPageMeta[0] + self.headerStruct.size + 8)
		binData = self.handle.read(self.cursorPageMeta[1])

		if self.cursorPageMeta[4] == "bz2 ":

			self.cursorPagePlain = bz2.decompress(binData)
			if len(self.cursorPagePlain) != self.cursorPageMeta[3]:
				raise Exception("Extracted data has incorrect length")
			localCursor = self.virtualCursor - self.cursorPageMeta[2]
			retStr = self.cursorPagePlain[localCursor: localCursor+bytes]
			self.virtualCursor += len(retStr)
			return retStr
			
		raise Exception("Not implemented compression:" + self.cursorPageMeta[4])

	def tell(self):
		return self.virtualCursor

	def seek(self, pos):
		self.virtualCursor = pos

	def _get_uncompressed_length(self):

		#Handle empty pages file
		self.handle.seek(0, 2)
		binaryLen = self.handle.tell()
		if binaryLen == 8:
			return 0

		#Find start of last page
		footerSize = 8 + self.footerStruct.size
		self.handle.seek(-footerSize, 2)
		endData = self.handle.read(footerSize)
		if endData[-4:] != "fini":
			raise Exception("File format not recognised")
		if endData[-8:-4] != "pend":
			raise Exception("File format not recognised")

		compressedDataLen = self.footerStruct.unpack(endData[:self.footerStruct.size])[0]

		#Send to start of last page
		self.handle.seek(-footerSize - compressedDataLen - self.headerStruct.size - 8, 2)
		lastPageMeta = self._parse_header_at_cursor()
		self.plainLen = lastPageMeta[2] + lastPageMeta[3]

	def __len__(self):
		if self.plainLen is None:
			self._get_uncompressed_length()

		return self.plainLen

	def _write_page(self, data):
		if self.method == "bz2":
			return self._write_page_bz2(data)
		raise Exception("Not implemented compression:" + self.method)

	def _write_page_bz2(self, data):
		import bz2
		compressedData = bz2.compress(data)
		print "Write page", len(data), ", compressed size", len(compressedData)

		#Header
		self.handle.write("page")
		header = self.headerStruct.pack(len(data), len(compressedData), self.writtenPlainBytes)
		self.handle.write(header)
		self.handle.write("bz2 ")

		#Copy data
		self.handle.write(compressedData)
		self.writtenPlainBytes += len(data)

		#Footer
		footer = self.footerStruct.pack(len(compressedData))
		self.handle.write(footer)
		self.handle.write("pend")

