# Databricks notebook source
class ApiHandler:
    def __init__(self, url):
        self.url = url
    
    def fetch_data(self):
        # データをAPIから取得
        data = requests.get(self.url).json()
        return data
    
    def process_data(self, data):
        x = [item['value'] for item in data]
        return sum(x) / len(x)


# COMMAND ----------

class MathOperations:
    def add(self, a, b):
        return a + b
    
    def divide(self, a, b):
        result = a / b  # ゼロ除算の可能性を考慮していない
        return result


# COMMAND ----------

# MAGIC %sql
# MAGIC select 1
