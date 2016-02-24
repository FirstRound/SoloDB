class HashTable:
	name = ""
	items = dict()
	hash_size = 40
	offset_size = 10

	def __init__(self):
		self.items = dict()

	def add_item(self, key, value):
		self.items[key] = value