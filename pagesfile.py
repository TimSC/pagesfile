import bz2, struct, os, copy, gzip

class PagesFileLowLevel(object):
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

		#self.method = "bz2 "
		self.method = "zlib"
		self.virtualCursor = 0
		self.pageStep = 1000000
		self.useTrashThreshold = 0.9

		#Index of on disk pages
		self.pageIndex = {}
		self.pageTrash = []

		#inUse, uncompSize, compSize, uncompPos, allocSize
		self.headerStruct = struct.Struct(">BQQQQ")

		self.footerStruct = struct.Struct(">Q")
		self.plainLen = 0

		if createFile:
			self._init_file_structure()
		else:
			self._refresh_page_index()

	def _init_file_structure(self):
		self.handle.seek(0)
		self.handle.write("pset")
		self.handle.write(struct.pack(">QQ", self.plainLen, self.pageStep))

	def __del__(self):
		self.flush()
		
	def flush(self):
		self.handle.seek(4)
		self.handle.write(struct.pack(">QQ", self.plainLen, self.pageStep))

	def write(self, data):
		while len(data) > 0:
			meta = self._get_page_for_index(self.virtualCursor)
			if meta is None:
				meta = {}
				meta['pagePos'] = None
				meta['compSize'] = None
				meta['uncompPos'] = self.virtualCursor - (self.virtualCursor % self.pageStep)
			 	meta['uncompSize'] = self.pageStep
				meta['method'] = self.method
				meta['allocSize'] = None
			
				pageCursor = self.virtualCursor - meta['uncompPos']
				bytesRemainingInPage = meta['uncompSize'] - pageCursor
				bytes = len(data)
				if bytes > bytesRemainingInPage:
					bytes = bytesRemainingInPage

				plain = bytearray("".join("\x00" for i in range(self.pageStep)))
				plain[pageCursor:pageCursor+bytes] = data[:bytes]
				data = data[bytes:]
				self.virtualCursor += bytes
				
				self._write_page_to_disk(meta, plain)

				if self.virtualCursor > self.plainLen:
					self.plainLen = self.virtualCursor 
				continue

			#Check if entire page written
			entire = self.virtualCursor == meta['uncompPos'] and len(data) > meta['uncompSize']
			
			if entire:
				plain = data[:meta['uncompSize']]
				data = data[meta['uncompSize']:]
				self.virtualCursor += meta['uncompSize']
			else:
				plain = bytearray(self._read_entire_page(meta))
				pageCursor = self.virtualCursor - meta['uncompPos']
				bytesRemainingInPage = meta['uncompSize'] - pageCursor
				bytes = len(data)
				if bytes > bytesRemainingInPage:
					bytes = bytesRemainingInPage
				
				plain[pageCursor:pageCursor+bytes] = data[:bytes]
				data = data[bytes:]
				self.virtualCursor += bytes

			self._write_page_to_disk(meta, plain)

			if self.virtualCursor > self.plainLen:
				self.plainLen = self.virtualCursor 

	def _refresh_page_index(self):
		self.handle.seek(0)
		if self.handle.read(4) != "pset":
			raise Exception("File format not recognised")

		self.plainLen = struct.unpack(">Q", self.handle.read(8))[0]
		self.pageStep = struct.unpack(">Q", self.handle.read(8))[0]
		self.pageIndex = {}
		self.pageTrash = []
		while True:
			meta = self._parse_header_at_cursor()
			if meta is None:
				break
			#print "meta", meta
			if meta['inUse']:
				self.pageIndex[meta['uncompPos']] = meta
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

		#Seek for suitable page on disk
		expectedPageStart = pos - (pos % self.pageStep)
		if expectedPageStart in self.pageIndex:
			return self.pageIndex[expectedPageStart]

		return None

	def _read_entire_page(self, meta):

		self.handle.seek(meta['pagePos'] + self.headerStruct.size + 8)
		binData = self.handle.read(meta['compSize'])

		if meta['method'] == "bz2 ":
			import bz2
			plainData = bz2.decompress(binData)
			if len(plainData) != meta['uncompSize']:
				raise Exception("Extracted data has incorrect length")
			return plainData

		if meta['method'] == "zlib":
			import zlib
			plainData = zlib.decompress(binData)
			if len(plainData) != meta['uncompSize']:
				raise Exception("Extracted data has incorrect length")
			return plainData

		raise Exception("Not implemented")

	def read(self, bytes):
		meta = self._get_page_for_index(self.virtualCursor)
		if meta is None:

			if self.virtualCursor < 0 or self.virtualCursor >= self.plainLen:
				return ""

			#Handle if we are reading within a file but no page available
			#Look for start of next real page
			nextPageStartPos = self.virtualCursor - (self.virtualCursor % self.pageStep) + self.pageStep
			while nextPageStartPos not in self.pageIndex and nextPageStartPos < bytes + self.virtualCursor:
				nextPageStartPos += self.pageStep

			bytesBeforePage = nextPageStartPos - self.virtualCursor
			#print "bytesBeforePage", bytesBeforePage
			if bytesBeforePage < bytes:
				bytes = bytesBeforePage

			bytesRemainingInFile = self.plainLen - self.virtualCursor 
			if bytes > bytesRemainingInFile:
				bytes = bytesRemainingInFile	

			self.virtualCursor += bytes
			return "".join("\x00" for i in range(bytes))

		#Read a page from disk
		plain = self._read_entire_page(meta)
		
		pageCursor = self.virtualCursor - meta['uncompPos']
		bytesRemainingInPage = len(plain) - pageCursor
		if bytes > bytesRemainingInPage:
			bytes = bytesRemainingInPage

		bytesRemainingInFile = self.plainLen - self.virtualCursor 
		if bytes > bytesRemainingInFile:
			bytes = bytesRemainingInFile		

		self.virtualCursor += bytes

		return plain[pageCursor:pageCursor+bytes]

	def tell(self):
		return self.virtualCursor

	def seek(self, pos, mode=0):
		if mode == 0:
			if pos < 0:
				raise IOError("Invalid argument")
			self.virtualCursor = pos
			return

		if mode == 1:
			if self.virtualCursor + pos < 0:
				raise IOError("Invalid argument")
			self.virtualCursor += pos
			return

		if mode == 2:
			if self.plainLen + pos < 0:
				raise IOError("Invalid argument")
			self.virtualCursor = self.plainLen + pos
			return

	def __len__(self):
		return self.plainLen

	def _write_page_to_disk(self, meta, plain):

		encodedData = None

		if meta['method'] == "bz2 ":
			import bz2
			encodedData = bz2.compress(plain)

		if meta['method'] == "zlib":
			import zlib
			encodedData = zlib.compress(str(plain))

		if encodedData == None:
			raise Exception("Not implemented compression:" + meta['method'])


		if meta['uncompPos'] not in self.pageIndex:
			self.pageIndex[meta['uncompPos']] = meta

		#Does this fit in original location
		if meta['pagePos'] is not None and len(encodedData) <= meta['compSize']:
			pass
			#print "Write page at existing position"

		else:
			if meta['pagePos'] is not None:
				#Free old location
				self._set_page_unused(meta)
				trashMeta = copy.deepcopy(meta)
				self.pageTrash.append(trashMeta)

			#Try to use a trash page
			bestTPage = None
			bestSize = None
			bestIndex = None
			for i, tpage in enumerate(self.pageTrash):
				if tpage['allocSize'] < len(encodedData):
					continue #Too small
				if tpage['allocSize'] * self.useTrashThreshold > len(encodedData):
					continue #Too too big
				if bestSize is None or tpage['allocSize'] < bestSize:
					bestSize = tpage['allocSize']
					bestTPage = tpage
					bestIndex = i

			if bestTPage is not None:
				#print "Write existing page to larger area"
				#Write to trash page
				meta['pagePos'] = bestTPage['pagePos']
				meta['allocSize'] = bestTPage['allocSize']
				del self.pageTrash[bestIndex]
			else:
				#print "Write existing page at end of file"
				#Write at end of file
				self.handle.seek(0, 2)
				meta['pagePos'] = self.handle.tell()
				meta['allocSize'] = len(encodedData)

		meta['compSize'] = len(encodedData)

		#Write to disk
		self._write_data_page(meta, plain, encodedData)

	def _set_page_unused(self, meta):

		self.handle.seek(meta['pagePos'])
		#print "Set page to unused"

		#Header
		self.handle.write("page")
		header = self.headerStruct.pack(0x00, 0, 0, 0, meta['allocSize'])
		self.handle.write(header)
		self.handle.write("free")

		#Leave footer unchanged

	def _write_data_page(self, meta, data, encoded):

		self.handle.seek(meta['pagePos'])
		#print "Write page", meta['uncompPos'], ", compressed size", len(encoded)

		#Header
		self.handle.write("page")
		header = self.headerStruct.pack(0x01, meta['uncompSize'], meta['compSize'], meta['uncompPos'], meta['allocSize'])
		self.handle.write(header)
		self.handle.write(meta['method'])

		#Copy data
		self.handle.write(encoded)

		#Footer
		self.handle.seek(meta['pagePos'] + 8 + self.headerStruct.size + meta['allocSize'])
		footer = self.footerStruct.pack(meta['allocSize'])
		self.handle.write(footer)
		self.handle.write("pend")

class PagesFile(object):

	def __init__(self):
		self.virtualCursor = 0

		#Index of in memory pages
		self.pagesPlain = []
		self.pagesMeta = []
		self.pagesChanged = []


	def __del__(self):
		self.flush()
		
	def flush(self):


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


	def _get_page_for_index(self, pos):

		#Check for suitable page already in memory
		for i, page in enumerate(self.pagesMeta):
			if pos >= page['uncompPos'] and pos < page['uncompPos'] + page['uncompSize']:
				return i

		return None

	def _add_page(self, pos, plainLen):
		self.pagesMeta.append({'pagePos': None, 'compSize': None, 'uncompPos': pos,
			 'uncompSize': plainLen, 'method': self.method, 'allocSize': None})
		self.pagesChanged.append(True)
		self.pagesPlain.append(bytearray("".join("\x00" for i in range(plainLen))))

	def read(self, bytes):
		pass

	def tell(self):
		return self.virtualCursor

	def seek(self, pos, mode=0):
		if mode != 0:
			raise Exception("Not implemented")

		self.virtualCursor = pos

	def __len__(self):
		pass

if __name__ == "__main__":

	pf = PagesFileLowLevel("test.pages")	
	pf.write("stuffandmorestuffxx5u4u545ugexx")
	pf.seek(0)
	print "readback", pf.read(5)

	pf.seek(999990)
	pf.write("thecatsatonthematthequickbrownfoxjumpedoverthelazybrowncow")
	pf.seek(999990)
	print "a", pf.read(20)
	print "b", pf.read(20)

	pf.seek(1500000)
	pf.write("foo42t245u54u45u")

	pf.seek(1500000)
	test = pf.read(6)
	print "'"+str(test)+"'"

	pf.seek(2500000)
	pf.write("bar")

	pf.seek(10000000)
	pf.write("bar243y37y3")

	pf.seek(9000000)
	print len(str(pf.read(10)))
	

	pf.flush()
	print "len", len(pf)

	pf._refresh_page_index()

