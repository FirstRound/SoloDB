from Field import Field
from TableHeader import TableHeader
from HashTable import HashTable

class GlobalController:
	indices = dict() # indices[table_name].items[hash] = value
	input = ""
	output = ""
	tables = dict() #tables[table_name] = TableHeader

	in_b = 0

	def __init__(self, filename):
		self.output = open(filename, "r+")
		self.input = open(filename, "r")

	def save_schema_to_file(self):
		self.output.write(str(len(self.tables)) + "\n")
		for table in self.tables:
			self.output.write(table + "\n")
			self.output.write(str(len(self.tables[table].fields)) + "\n")
			for field in self.tables[table].fields:
				self.output.write(str(field.name) + ":" + str(field.type) + ":" + str(field.size) + "\n")

	def get_schema(self):
		tables = dict()
		tables_count = int(self.input.readline())
		for table in range(0, tables_count):
			fields = []
			pk = ""
			name = self.input.readline()[:-1]
			offset = int(self.input.readline()[:-1])
			count = int(self.input.readline()[:-1])
			has_pk = int(self.input.readline()[:-1])
			fields_count = int(self.input.readline())
			for field in range(0, fields_count):
				n, t, s = self.input.readline().split(":")
				fields.append(Field(n, t, int(s)))
			if (has_pk == 1):
				pk = self.input.readline()[:-1]
			tables[name] = TableHeader(name, fields)
			tables[name].set_data_offset(offset)
			tables[name].set_record_count(count)
			tables[name].set_primary_key(pk)

		self.tables = tables
		self.in_b = self.input.tell()

	def get_indices(self):
		tables = ["article", "keywords", "author", "conference"]
		for table in tables:
			if self.tables[table].primary_key == "":
				continue
			current = self.indices[table] = HashTable()
			size = self.tables[table].record_count
			for hash in range(0, size):
				h = self.input.read(HashTable.hash_size)
				o = int(self.input.read(HashTable.offset_size))
				current.add_item(h, o)

	def iterate_by_hashtable(self, table_name, clause, tar_table, key, value):
		s= []
		r = open("tmp.txt", "w")
		for hash in self.indices[table_name].items:
			#print(self.indices[table_name].items[hash])
			s.append(self.indices[table_name].items[hash])
			data = self.get_value_by_position(self.indices[table_name].items[hash], self.tables[table_name].record_size)
			
			if (table_name == tar_table):
				if (clause == "where"):
					if (self.tables[table_name].get_field(key, data).strip(" ") != value.strip(" ")):
						continue;
				if (clause == "like"):
					f = False
					for h in v:
						if (h.strip(" ") in self.tables[table_name].get_field(key, data).strip(" ")):
							f = True;
					if (f == False):
						continue
			yield data

	def get_data_by_hashtable(self, table_name,  hash):
		if hash not in self.indices[table_name].items:
			return None
		self.indices[table_name].items[hash]
		data = self.get_value_by_position(self.indices[table_name].items[hash], self.tables[table_name].record_size)
		return data

	def iterate_by_offset(self, table_name, clause, tar_table, key, value):
		offset = self.tables[table_name].data_offset
		size = self.tables[table_name].record_size
		for i in range(0, self.tables[table_name].record_count):
			data = self.get_value_by_position(offset+i*size, self.tables[table_name].record_size)
			if (table_name == tar_table):
				if (clause == "where"):
					if (self.tables[table_name].get_field(key, data).strip(" ") != value.strip(" ")):
						continue;
				if (clause == "like"):
					f = False
					for h in v:
						if (h.strip(" ") in self.tables[table_name].get_field(key, data).strip(" ")):
							f = True;
					if (f == False):
						continue
			yield data
		
	def get_value_by_position(self, pos, size):
		self.input.seek(pos, 0)
		data = self.input.read(size)
		return data
		
	def join(self, string, tokens):
		k = ""
		v = ""
		tab = ""
		fie = ""
		if tokens[4].lower() == "where" or tokens[4].lower() == "like":
			k, v = tokens[5].split("=")
			tab, fie = k.split(".")
		v = v.replace("&", " ")
		if (tokens[4].lower() == "like"):
			v = v.split(" ")


		tables = string.split("|")
		res = TableHeader("tmp", self.tables[tables[0]].fields)
		values = None
		if self.tables[tables[0]].primary_key == "":
			values = self.iterate_by_offset(tables[0], tokens[4].lower(), tab, fie, v)
		else:
			values = self.iterate_by_hashtable(tables[0], tokens[4].lower(), tab, fie, v)

		c = []

		for tm in values:
			c.append(tm)
		values = c

		tables.remove(tables[0])
		for t in tables:
			tmp = []
			d = []
			common = self.tables[t].get_common_field(res.fields)
			
			if (self.tables[t].primary_key == common):
				for i in values:
					string = res.get_field(common, i)
					#print(string)
					data = self.get_data_by_hashtable(self.tables[t].name, string)
					if (t == tab):
						if tokens[4].lower() == "where":

							if (self.tables[t].get_field(fie, data).strip(" ") != v.strip(" ")):
								continue
						if tokens[4].lower() == "like":
							f = False
							for h in v:
								if (h.strip(" ") in self.tables[t].get_field(fie, data).strip(" ")):
									print(self.tables[t].get_field(fie, data).strip(" "))
									f = True;
							if (f == False):
								continue
					if (data != None):
						ass = i;
						for h in self.tables[t].fields:
							if (h.name != common):
								ass += self.tables[t].get_field(h.name, data)
						d.append(ass)
			values = d

			res.add_fields(self.tables[t].fields)
			res.remove_field(common)

		return res, values
			

	def query(self, string):
		#[COMAND {[FIELD,FIELD]|*}] [TABLE|TABLE|..] [CLAUSE {KEY=VALUE] | [LIMIT(FROM,TO)] | [GROUP BY FIELD]
		result = []
		fields = []
		dictionary = []
		tokens = string.split(" ")
		if tokens[0].lower() == "select":
			values = None
			table = None
			table, values = self.join(tokens[3], tokens)
			if "distinct" in string.lower():
				unique = set()
				for i in values:
					unique.add(i)
				values = list(unique)
			for i in values:
				dictionary.append(table.to_dict(i))
			values = dictionary
			if "order" in string.lower():
				s1, s2 = string.split("[")
				s1, s2 = s2.split("]")
				flag = False
				if "order_down" in string.lower():
					flag = True
				values = sorted(values, key = lambda d: d[s1], reverse=flag)
			for i in values:
				result.append(i)
			if (tokens[1] != "*"):
				ans = []
				fields = tokens[1].split(",")
				for i in result:
					val = list()
					for j in fields:
						val.append(i[j])
					ans.append(val)
				result = ans
			else:
				res = list()
				fields = table.fields	
				for i in result:
					val = list()
					for j in fields:
						val.append(i[j.name])
					res.append(val)
				result = res
		if "limit" in string.lower():
			s1, s2 = string.split("(")
			s1, s2 = s2.split(")")
			s1, s2 = s1.split(",")
			result = result[int(s1):]
			result=result[:int(s2)]

		return result

	def change_indices(self):
		wr = open("db.txt", "a")
		print(wr.tell())
		tables = ["article", "keywords", "author", "conference"]
		self.output.seek(self.in_b, 0)  #450
		self.input.seek(self.in_b, 0)  #450
		point = wr.tell()
		for table in tables:
			if self.tables[table].primary_key == "":
				continue
			for i in range(0, self.tables[table].record_count):
				h = self.input.read(HashTable.hash_size)
				o = self.input.read(HashTable.offset_size)				
				o = str(point).ljust(10)
				point += self.tables[table].record_size
				self.output.write(h + o)
				#self.wr.write(data)
		#wr.close()

	def print_schema(self):
		for i in self.tables:
			print(self.tables[i].name)
			print(self.tables[i].data_offset)
			print(self.tables[i].record_count)
			print(self.tables[i].fields_count)
			for j in self.tables[i].fields:
				print(j.name + ":" + j.type + ":" + str(j.size))
			print("PK: " + self.tables[i].primary_key)

	def print_tab_names(self):
		for table in self.tables:
			print(table)

	def print_indices(self, name):
		for j in self.indices[name].items:
			print(j + ": " + str(self.indices[name].items[j]))

	def debug(self):
		s = []
		print(len(self.indices["conference"].items))
		for hash in self.indices["conference"].items:
			s.append(self.indices["conference"].items[hash])
		print(min(s))

	def init_project_schema(self):
		fields = []

		f1 = Field("title", "text", 256);
		fields.append(f1);
		f2 = Field("doi", "text", 32);
		fields.append(f2);
		f3 = Field("pdf", "text", 64);
		fields.append(f3);
		f4 = Field("a_id", "text", 40);
		fields.append(f4);

		table = TableHeader("article", fields)
		self.tables["article"] = (table)

		fields2 = []
		k1 = Field("k_id", "text", 40);
		fields2.append(k1);
		k2 = Field("a_id", "text", 40);
		fields2.append(k2);
		table2 = TableHeader("k_t_a", fields2)

		self.tables["k_t_a"] = table2

gc = GlobalController("db.txt")
gc.get_schema()
gc.get_indices()
#s = gc.query("SELECT title,doi FROM article WHERE doi=10.1109/ICIME.2010.5478195")
#s = gc.query("SELECT * FROM article ;")
key = "FUZZY LOGIC"
s = gc.query("select title,doi,pdf,a_id from k_t_a|article|keywords LIKE keywords.word=%s LIMIT(0,10)  distinct" % (key,))


for i in s:
	print(i)


#gc.init_project_schema()
#gc.save_schema_to_file()

#gc.get_schema()
#gc.print_schema()
#gc.print_tab_names()

#gc.get_indices()
#gc.print_indices("article")

#gc.iterate_by_hashtable("conference")
#gc.iterate_by_offset("a_t_au")

#gc.change_indices()

#gc.debug()

#last position
#r = open("db.txt", "a")
#print(str(r.tell()).ljust(10))



