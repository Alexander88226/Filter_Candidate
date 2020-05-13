import mysql.connector as mysqlconnector
from sqlalchemy import create_engine
from psycopg2 import connect, DatabaseError
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd


# MySQL configuration
MySQL_User = 'root'
MySQL_Password = 'password'
MySQL_Host = '127.0.0.1'

# DataBase Name
DB_Name = 'candidatesdb'
# Table Name
FilterCriteria_TableName = "filtercriteria"
Candidates_TableName = "candidates"

FilterCriteria_Columns = ['Start_Date', 'End_date', 'IS_NP', 'OOS_NP', 'OOS_IS_Avg_Trade', 'ALL_Robustness_Index', 'ALL_NP_DD_Ratio', 'IS_Avg_Trade', \
        'IS_Trades_Per_Year', 'OOS_Trades_Per_Year', 'OOS_Total_Trades', 'Duplicity']


# Class of Filter Criteria
class FilterCriteria(object):
    # constructor
    def __init__(self, IS_NP = 0, OOS_NP = 0, OOS_IS_Avg_Trade=80, 
                 ALL_Robustness_Index = 80, ALL_NP_DD_Ratio = 2, IS_Avg_Trade = 60, IS_Trades_Per_Year = 40,
                OOS_Trades_Per_Year = 40, OOS_Total_Trades = 10, Duplicity= 90):
        self.IS_NP                  = IS_NP
        self.OOS_NP                 = OOS_NP
        self.OOS_IS_Avg_Trade       = OOS_IS_Avg_Trade
        self.ALL_Robustness_Index   = ALL_Robustness_Index
        self.ALL_NP_DD_Ratio        = ALL_NP_DD_Ratio
        self.IS_Avg_Trade           = IS_Avg_Trade
        self.IS_Trades_Per_Year     = IS_Trades_Per_Year
        self.OOS_Trades_Per_Year    = OOS_Trades_Per_Year
        self.OOS_Total_Trades       = OOS_Total_Trades
        self.Duplicity              = Duplicity
    
    # build dictinary data from object
    def to_dict(self):
        return {
            'IS_NP'                 : self.IS_NP,
            'OOS_NP'                : self.OOS_NP,
            'OOS_IS_Avg_Trade'      : self.OOS_IS_Avg_Trade,
            'ALL_Robustness_Index'  : self.ALL_Robustness_Index,
            'ALL_NP_DD_Ratio'       : self.ALL_NP_DD_Ratio,
            'IS_Avg_Trade'          : self.IS_Avg_Trade,
            'IS_Trades_Per_Year'    : self.IS_Trades_Per_Year,
            'OOS_Trades_Per_Year'   : self.OOS_Trades_Per_Year,
            'OOS_Total_Trades'      : self.OOS_Total_Trades,
            'Duplicity'             : self.Duplicity,
        }
    
    # set criteria from dataframe
    def from_dataframe(self, dataframe):
        self.criteria = dataframe

    # store filter criteria into DB
    def storeFilterCriteriaToDB(self, file_path, table_name="filtercriteria", con=None):
        self.criteria.to_sql(table_name, con, if_exists='replace')
    
    # get filter criteria from Excel file
    @classmethod
    def getFilterCriteriaFromDB(cls, file_path, table_name="filtercriteria", con=None):
        exists = checkExistsFilterCriteria(file_path)
        if exists:
            criteria = pd.read_sql("select * from " + FilterCriteria_TableName ,con=con)
            return criteria
        return None
        
    @classmethod
    def fromDict(cls, d):
        df = {k : v for k, v in d.items() if k in FilterCriteria_Columns}
        return cls(**df)

# get Filter Criteria from file
def getFilterCriteriaFromDB(file_path):
    # create engine to load and store dataframe with mysql and postgresql
    db_connection = createDBConnection()
    # load Filter Criteria from DB    
    criteria = FilterCriteria.getFilterCriteriaFromDB(file_path, FilterCriteria_TableName, db_connection)
    if criteria is None: # if Filter Criteria File doesn't exist
        # Create Default Filter Criteria
        filter_criteria = FilterCriteria()
        # get DataFrame from Default Filter Criteria
        df = pd.DataFrame.from_records([filter_criteria.to_dict()])
        filter_criteria.from_dataframe(df)
        # store Filter Criteria into Excel file
        filter_criteria.storeFilterCriteriaToDB(file_path, FilterCriteria_TableName, db_connection)

    else: # if Filter Criteria exists, read it
        # convert DataFrame to Class object
        criteria_dict = criteria.T.to_dict().values()
        for cri in criteria_dict:
            filter_criteria = FilterCriteria.fromDict(cri)
            filter_criteria.criteria = criteria
            break
    return filter_criteria

# Create DB Connection engine
def createDBConnection():
    # generate db connection string
    db_connection_str = 'mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(MySQL_User, MySQL_Password, MySQL_Host, DB_Name)
    # create db connection
    db_connection = create_engine(db_connection_str)
    return db_connection


# Create DataBase for mysql and postgresql
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

# store DataFrame in excel or MySQL or PostgreSQL
def storeDataFrameInDB(file_path, dataframe, table_name):
    db_connection = createDBConnection()
    dataframe.to_sql(table_name, db_connection, if_exists='replace')

# check if Filter Criteria table exists in file or database of MySQL and PostgreSQL
def checkExistsFilterCriteria(file_path):
    conn = connectDataBase(db_name=DB_Name)
    cur = conn.cursor(buffered=True)
    cur.execute("select * from information_schema.tables where table_name=%s", (FilterCriteria_TableName,))
    exists = cur.rowcount
    cur.close()
    conn.close()
    return bool(exists)
