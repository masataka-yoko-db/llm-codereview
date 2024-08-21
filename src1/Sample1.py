# Databricks notebook source
class DataProcessor:
    def __init__(self, data):
        self.data = data
        
    def clean_data(self):
        cleaned = [d for d in self.data if d is not None and d != '']  # 余分な空白を除去
        return cleaned
        
    def calculate_average(self):
        cleaned = self.clean_data()
        total = sum(cleaned)
        avg = total / len(cleaned)  # 全体の平均を計算
        return avg

# COMMAND ----------

class FileManager:
	def __init__(self, file_name):
		self.file_name = file_name
		self.file = None
		
	def open_file(self):
		self.file = open(self.file_name, 'r')
		contents = self.file.read()
		self.file.close()
		return contents
		
	def write_file(self, data):
		self.file = open(self.file_name, 'w')
		# 書き込み処理を実施
		unused_variable = "not_used"
		self.file.write(data)
		self.file.close()

# COMMAND ----------

# MAGIC %md
# MAGIC * test
