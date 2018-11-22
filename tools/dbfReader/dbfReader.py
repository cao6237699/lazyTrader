# -*- encoding:utf-8 -*-

"""
可以正确读取并输出中文字段及内容的dbf类
"""
import struct
import datetime
import decimal
import itertools
import csv
from cStringIO import StringIO
from operator import itemgetter

class DBF(object):
	"""
	从dbf文件读取数据并转换为csv、excel文件的类
	例：
	   import dbfReader
	   filename = 'C:\\Users\\Desktop\\dbfReader\\haha.dbf'
	   dbf = dbfReader.DBF(filename)
	   dbf.to_csv('haha.csv')
	"""
# ----------------------------------------------------------------------------------------
	def __init__(self,filename):
		""" constructed function"""
		
		super(DBF, self).__init__()
		self.filename = filename
		with open(filename,'rb') as f:
			self.fileData = list(self.dbfreader(f))
# ----------------------------------------------------------------------------------------	
	def dbfreader(self,f):
		"""Returns an iterator over records in a Xbase DBF file.

		The first row returned contains the field names.
		The second row contains field specs: (type, size, decimal places).
		Subsequent rows contain the data records.
		If a record is marked as deleted, it is skipped.
	    
		File should be opened for binary reads.
	    
		"""
		# See DBF format spec at:
		# http://www.pgts.com.au/download/public/xbase.htm#DBF_STRUCT
	    
		numrec, lenheader = struct.unpack('<xxxxLH22x', f.read(32))
		numfields = (lenheader - 33) // 32
	    
		fields = []
		for fieldno in xrange(numfields):
		    name, typ, size, deci = struct.unpack('<11sc4xBB14x', f.read(32))
		    name = name.replace('\0', '')  # eliminate NULs from string
		    fields.append((name, typ, size, deci))
		yield [field[0] for field in fields]
		yield [tuple(field[1:]) for field in fields]
	    
		terminator = f.read(1)
		assert terminator == '\r'
	    
		fields.insert(0, ('DeletionFlag', 'C', 1, 0))
		fmt = ''.join(['%ds' % fieldinfo[2] for fieldinfo in fields])
		fmtsiz = struct.calcsize(fmt)
		for i in xrange(numrec):
		    record = struct.unpack(fmt, f.read(fmtsiz))
		    if record[0] != ' ':
			continue  # deleted record
		    result = []
		    for (name, typ, size, deci), value in itertools.izip(fields, record):
			if name == 'DeletionFlag':
			    continue
			if typ == "N":
			    value = value.replace('\0', '').lstrip()
			    if value == '':
				value = 0
			    elif deci:
				value = decimal.Decimal(value)
			    else:
				value = int(value)
			elif typ == 'D':
			    y, m, d = int(value[:4]), int(value[4:6]), int(value[6:8])
			    value = datetime.date(y, m, d)
			elif typ == 'L':
			    value = (value in 'YyTt' and 'T') or (value in 'NnFf' and 'F') or '?'
			elif typ == 'F':
			    value = float(value)
			result.append(value)
		    yield result
# ----------------------------------------------------------------------------------------
	def to_csv(self,filename):
		"""convert dbf data to csv file"""
		
		fieldnames, fieldspecs, records = self.fileData[0], self.fileData[1], self.fileData[2:]
		io = StringIO()
		with open(filename,'wb') as f:
		    fwriter = csv.writer(f)
		    fwriter.writerow(fieldnames)
		    fwriter.writerows(records)
