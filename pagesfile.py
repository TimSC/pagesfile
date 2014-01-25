import bz2, struct, os

class PagesFile(object):
	def __init__(self, fi):
		createFile = False
		if isinstance(fi, str):
			createFile = not os.path.isfile(fi)
			if createFile:
				self.handle = open(fi, "w+b")
				createFile = True
			else:
				self.handle = open(fi, "r+b")
		else:
			self.handle = fi

		self.method = "bz2 "
		self.virtualCursor = 0

		self.pageIndex = [] #Index of on disk pages

		#Index of in memory pages
		self.pagesPlain = []
		self.pagesMeta = []
		self.pagesChanged = []
		self.pageTrash = []

		#inUse, uncompSize, compSize, uncompPos, allocSize
		self.headerStruct = struct.Struct(">BQQQQ")

		self.footerStruct = struct.Struct(">Q")
		
		self.plainLen = 0
		self.pageAllocLen = 0

		if createFile:
			self._init_file_structure()
		else:
			self._refresh_page_index()

	def _init_file_structure(self):
		self.handle.seek(0)
		self.handle.write("pset")
		self.handle.write(struct.pack(">Q", self.plainLen))

	def __del__(self):
		self.flush()
		
	def flush(self):
		self.handle.seek(4)
		self.handle.write(struct.pack(">Q", self.plainLen))

		for i, changed in enumerate(self.pagesChanged):
			if not changed:
				continue
			self._write_page_to_disk(i)


	def write(self, data):
		#http://www.skymind.com/~ocrow/python_string/

		while len(data)>0:

			pageNum = self._get_page_for_index(self.virtualCursor)
			print len(data), pageNum
		
			if pageNum is None:
				pageStep = 1000000
				pageStart = self.virtualCursor - (self.virtualCursor % pageStep)
				self._add_page(pageStart, pageStep)
				continue
			
			meta = self.pagesMeta[pageNum]
			plainPage = self.pagesPlain[pageNum]
			self.pagesChanged[pageNum] = True
			localIndex = self.virtualCursor - meta['uncompPos']
			spaceOnPage = meta['uncompSize'] - localIndex
			dataToWriteThisPage = len(data)
			if dataToWriteThisPage > spaceOnPage:
				dataToWriteThisPage = spaceOnPage

			plainPage[localIndex:localIndex+dataToWriteThisPage] = data[:dataToWriteThisPage]
			data = data[dataToWriteThisPage:]
			self.virtualCursor += dataToWriteThisPage

			#Update end point of file
			if self.virtualCursor > self.plainLen:
				self.plainLen = self.virtualCursor 

	def _add_page(self, pos, plainLen):
		self.pagesMeta.append({'pagePos': None, 'compSize': None, 'uncompPos': pos,
			 'uncompSize': plainLen, 'method': self.method, 'allocSize': None})
		self.pagesChanged.append(True)
		self.pagesPlain.append(bytearray("".join("\x00" for i in range(plainLen))))

	def _refresh_page_index(self):
		self.handle.seek(0)
		if self.handle.read(4) != "pset":
			raise Exception("File format not recognised")

		self.plainLen = struct.unpack(">Q", self.handle.read(8))[0]
		self.pageAllocLen = 0
		self.pageIndex = []
		self.pageTrash = []
		while True:
			meta = self._parse_header_at_cursor()
			if meta is None:
				break
			print "meta", meta
			if meta['inUse']:
				self.pageIndex.append(meta)
			else:
				self.pageTrash.append(meta)
			self.handle.seek(meta['allocSize'], 1)
			footerData = self.handle.read(8)
			endStr = self.handle.read(4)
			if endStr != "pend":
				raise Exception("File format not recognised")

	def _parse_header_at_cursor(self):
		pagePos = self.handle.tell()
		startStr = self.handle.read(4)
		if len(startStr) == 0:
			return None
		if startStr != "page":
			raise Exception("File format not recognised")

		header = self.handle.read(self.headerStruct.size)
		inUse, uncompSize, compSize, uncompPos, allocSize = self.headerStruct.unpack(header)
		method = self.handle.read(4)
		return {'inUse': inUse, 'pagePos': pagePos, 'compSize': compSize, 'uncompPos': uncompPos,
			 'uncompSize': uncompSize, 'method': method, 'allocSize': allocSize}

	def _get_page_for_index(self, pos):

		#Check for suitable page already in memory
		for i, page in enumerate(self.pagesMeta):
			if pos >= page['uncompPos'] and pos < page['uncompPos'] + page['uncompSize']:
				return i

		#Seek for suitable page on disk
		for i, page in enumerate(self.pageIndex):
			#print "check", page
			if pos >= page['uncompPos'] and pos < page['uncompPos'] + page['uncompSize']:
				return self._load_page_into_mem(page)

		return None

	def _load_page_into_mem(self, meta):
		self.handle.seek(meta['pagePos'] + self.headerStruct.size + 8)
		binData = self.handle.read(meta['compSize'])

		if meta['method'] == "bz2 ":
			plainData = bytearray(bz2.decompress(binData))
			if len(plainData) != meta['uncompSize']:
				raise Exception("Extracted data has incorrect length")

			self.pagesPlain.append(plainData)
			self.pagesMeta.append(meta)
			self.pagesChanged.append(False)

			return len(self.pagesPlain)-1
		else:
			raise Exception("Not implemented")

	def read(self, bytes):
		pageNum = self._get_page_for_index(self.virtualCursor)

		if pageNum is None:
			raise Exception("Undefined location in file")

		plain = self.pagesPlain[pageNum]
		meta = self.pagesMeta[pageNum]
		
		localCursor = self.virtualCursor - meta['uncompPos']
		bytesRemain = meta['uncompSize'] - localCursor
		if bytesRemain < bytes:
			bytes = bytesRemain
			
		return plain[localCursor:localCursor+bytes]

	def tell(self):
		return self.virtualCursor

	def seek(self, pos, mode=0):
		if mode != 0:
			raise Exception("Not implemented")

		self.virtualCursor = pos

	def __len__(self):
		return self.plainLen

	def _write_page_to_disk(self, i):

		plain = self.pagesPlain[i]
		meta = self.pagesMeta[i]

		if meta['method'] != "bz2 ":
			raise Exception("Not implemented compression:" + meta['method'])

		import bz2
		encodedData = bz2.compress(plain)
		
		#Decide where to write
		if meta['pagePos'] is None:
			print "Write new page at end of file"
			self.handle.seek(0, 2) #Write at end
			meta['pagePos'] = self.handle.tell()
			meta['allocSize'] = len(encodedData)
		else:
			#Does this fit in original location
			if len(encodedData) <= meta['compSize']:
				pass
				print "Write page at existing position"

			else:
				print "Write existing page at end of file"

				#Free old location
				self._set_page_unused(meta)

				#Write at end of file
				self.handle.seek(0, 2)
				meta['pagePos'] = self.handle.tell()
				meta['allocSize'] = len(encodedData)

		meta['compSize'] = len(encodedData)

		if meta['method'] == "bz2 ":
			self._write_page_bz2(meta, plain, encodedData)

		self.pagesChanged[i] = False

	def _set_page_unused(self, meta):
		print "Set page to unused"

		#Header
		self.handle.write("page")
		header = self.headerStruct.pack(0x00, 0, 0, 0, meta['allocSize'])
		self.handle.write(header)
		self.handle.write("bz2 ")

		#Leave footer unchanged

	def _write_page_bz2(self, meta, data, encoded):

		self.handle.seek(meta['pagePos'])
		print "Write page", len(data), ", compressed size", len(encoded)

		#Header
		self.handle.write("page")
		header = self.headerStruct.pack(0x01, meta['uncompSize'], meta['compSize'], meta['uncompPos'], meta['allocSize'])
		self.handle.write(header)
		self.handle.write("bz2 ")

		#Copy data
		self.handle.write(encoded)

		#Footer
		footer = self.footerStruct.pack(meta['allocSize'])
		self.handle.write(footer)
		self.handle.write("pend")

if __name__ == "__main__":

	pf = PagesFile("test.pages")	
	pf.write("stuffandmorestuffxxxx")
	pf.seek(0)
	print "readback", pf.read(5)

	pf.seek(1500000)
	pf.write("foo")

	pf.flush()
	print "len", len(pf)

