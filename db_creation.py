import mysql.connector as mysqlconnector
from sqlalchemy import create_engine
import pandas as pd


# MySQL configuration
MySQL_User = 'user'
MySQL_Password = 'password'
MySQL_Host = '127.0.0.1'

# DataBase Name
DB_Name = 'candidatesdb'



# Create DataBase for mysql
def createDataBase():
    cnx = connectDataBase()
    try:
        cursor = cnx.cursor()
        # create database if not exists
        cursor.execute('CREATE DATABASE IF NOT EXISTS ' + DB_Name)
        cnx.commit()
        cursor.close()
    except mysqlconnector.Error as err:
        print(err)
        exit(0)
    finally:
        if cnx is not None:
            cnx.close()

# connect DataBase of MySQL and PostgreSQL
def connectDataBase(db_name=""):
    cnx = None
    config = {
    'user': MySQL_User,
    'password': MySQL_Password,
    'host': MySQL_Host,
    }        
    if db_name:
        config['database'] = db_name
    try:
        cnx = mysqlconnector.connect(**config)
    except mysqlconnector.Error as err:
        print(err)
        exit(0)
    return cnx 

if __name__ == "__main__":
    createDataBase()
