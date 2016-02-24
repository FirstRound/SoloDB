from Field import Field
from TableHeader import TableHeader
from HashTable import HashTable
from GlobalController import GlobalController

class Inserter:

	dbname = "knbase"
	user = "postgres"
	password = "root"
	
	input = open("db.txt", "r")
	output = open("db.txt", "a")
	index = open("db.txt", "a")

	gc = None

	def __init__(self, gc):
		self.gc = gc
		index

	def iterate_by_hashtable(self, table_name):
		for hash in self.indices[table_name].items:
			#print(self.indices[table_name].items[hash])
			data = self.get_value_by_position(self.indices[table_name].items[hash], self.tables[table_name].record_size)
			#TODO data operation