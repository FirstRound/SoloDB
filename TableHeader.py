from Field import Field

class TableHeader:
	name = ""
	fields_count = 0
	fields = []
	record_size = 0
	data_offset = 0
	record_count = 0
	primary_key = ""

	def __init__(self, name, fields):
		self.name = name
		self.fields = fields
		self.fields_count = len(fields)
		for i in fields:
			self.record_size += i.size

	def set_data_offset(self, offset):
		self.data_offset = offset

	def set_record_count(self, count):
		self.record_count = count

	def set_primary_key(self, key):
		self.primary_key = key

	def get_field(self, f_name, string):
		offset = 0
		size = 0
		for i in self.fields:
			if (i.name != f_name):
				offset += i.size
			else:
				size = i.size
				break
		return string[offset:offset+size]

	def get_fields(self, fields, str):
		result = ""
		for field in fields:
			result += self.get_field(field, str)
		return result

	def add_fields(self, fields):
		for i in fields:
			self.fields.append(i)

		self.fields_count = len(self.fields)

		for i in fields:
			self.record_size += i.size

	def remove_field(self, name):
		s = None
		l = len(self.fields)
		for i in range(0, l):
			if (self.fields[l-i-1].name == name):
				s = self.fields[l-i-1]
				break
		self.fields.remove(s)

	def get_common_field(self, fields):
		for i in fields:
			for j in self.fields:
				if (i.name == j.name):
					return i.name



	def get_field_size(self, field):
		for i in self.fields:
			if (i.name == field):
				return i.size

	def to_tuple(self, fields, str):
		res = list()
		f = []
		off = 0
		if (fields == []):
			for j in self.fields:
				f.append(j.name)
		else:
			f = fields
		for i in f:
			size = self.get_field_size(i)
			res.append(str[off:off+size])
			off += size
		return res

	def to_dict(self, string):
		res = dict()
		for i in self.fields:
			f = self.get_field(i.name, string)
			res[i.name] = f
		return res


