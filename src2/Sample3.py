# Databricks notebook source
class UserManager:
    def __init__(self, users):
        self.users = users
    
    def get_user_names(self):
        names = []
        for user in self.users:
            names.append(user['name'])  # ユーザー名を抽出
        return names
    
    def add_user(self, user):
        self.users.append(user)


# COMMAND ----------

class TemperatureConverter:
    def celsius_to_fahrenheit(self, t):
        return t * 9/5 + 32
    
    def fahrenheit_to_celsius(self, t):
        return (t - 32) * 5/9

