# Load Libraries
import os, sys
import math
import pandas as pd
import codecs
from dateutil.parser import parse
from datetime import date
from decimal import Decimal
import mysql.connector as mysqlconnector
from sqlalchemy import create_engine
from psycopg2 import connect, DatabaseError
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from multiprocessing import Pool
from multiprocessing import freeze_support
from functools import partial

# check arguments length
if len(sys.argv) != 3:
    exit(0)
# first argv is Start Date
# second one is End Date
Start_Date = sys.argv[1]
End_Date = sys.argv[2]


# default Server Type is MySQL
# but if MySQL doesn't be installed or connection to MySQL is failed, Server Type will be excel and Candidates will be stored in Excel file

Server_Type = "mysql"

OOS_Percent = 0.2
IS_Percent = 1 - OOS_Percent
Duplicity_Candidate_Count = 100
IS_Result_File = 'IS.txt'
OOS_Result_File = 'OOS.txt'


# DataBase Name
DB_Name = 'candidatesdb'
# Table Name
FilterCriteria_TableName = "filtercriteria"
Candidates_TableName = "candidates"

# MySQL configuration
MySQL_User = 'user'
MySQL_Password = 'password'
MySQL_Host = '127.0.0.1'


# define Columns of DataFrame for each Data
Candidate_Attribute_Columns = ['POI_Switch', 'NATR', 'Fract', 'Filter1_Switch', 'Filter1_N1', 'Filter1_N2', 'Filter2_Switch', 'Filter2_N1', 'Filter2_N2']
Candidate_Columns = ['POI_Switch', 'NATR', 'Fract', 'Filter1_Switch', 'Filter1_N1', 'Filter1_N2', 'Filter2_Switch', 'Filter2_N1', 'Filter2_N2', \
            'Test', 'IS_TS_Index', 'IS_Net_Profit', 'IS_Total_Trades', 'IS_Profitable', 'IS_Avg_Trade', 'IS_Max_Intraday_Drawdown', 'IS_ProfitFactor', 'IS_Robustness_Index', \
            'OS_Net_Profit', 'OS_Total_Trades', 'OS_Profitable', 'OS_Avg_Trade', 'OS_Max_Intraday_Drawdown', 'OS_ProfitFactor', 'OS_Robustness_Index']

FilterCriteria_Columns = ['Start_Date', 'End_date', 'IS_NP', 'OOS_NP', 'OOS_IS_Avg_Trade', 'ALL_Robustness_Index', 'ALL_NP_DD_Ratio', 'IS_Avg_Trade', \
        'IS_Trades_Per_Year', 'OOS_Trades_Per_Year', 'OOS_Total_Trades', 'Duplicity']

Candidate_IS_Data_Columns = ['Test', 'TS Index', 'Net Profit', 'Total Trades', 'Profitable', 'Avg Trade', 'Max Intraday Drawdown', 'ProfitFactor', 'Robustness Index']
New_Candidate_IS_Data_Columns = ['Test', 'IS_TS_Index', 'IS_Net_Profit', 'IS_Total_Trades', 'IS_Profitable', 'IS_Avg_Trade', 'IS_Max_Intraday_Drawdown', 'IS_ProfitFactor', 'IS_Robustness_Index']

New_Candidate_IS_Data_Columns = Candidate_Attribute_Columns + New_Candidate_IS_Data_Columns
Candidate_OS_Data_Columns = ['Test', 'Net Profit', 'Total Trades', 'Profitable', 'Avg Trade', 'Max Intraday Drawdown', 'ProfitFactor', 'Robustness Index']
NEW_Candidate_OS_Data_Columns = ['Test', 'OS_Net_Profit', 'OS_Total_Trades', 'OS_Profitable', 'OS_Avg_Trade', 'OS_Max_Intraday_Drawdown', 'OS_ProfitFactor', 'OS_Robustness_Index']



# Class of Filter Criteria
class FilterCriteria(object):
    # constructor
    def __init__(self, IS_NP = 0, OOS_NP = 0, OOS_IS_Avg_Trade=70, 
                 ALL_Robustness_Index = 60, ALL_NP_DD_Ratio = 1, IS_Avg_Trade = 60, IS_Trades_Per_Year = 40,
                OOS_Trades_Per_Year = 40, OOS_Total_Trades = 10, Duplicity= 95):
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
    def storeFilterCriteriaToDB(self, file_path, table_name="filtercriteria", server_type='excel', con=None):
        if server_type == 'excel':
            self.criteria.to_excel(file_path, sheet_name=table_name)
        else: # server_type == 'mysql'
            self.criteria.to_sql(table_name, con, if_exists='replace')
    
    # get filter criteria from Excel file
    @classmethod
    def getFilterCriteriaFromDB(cls, file_path, table_name="filtercriteria", server_type='excel', con=None):
        exists = checkExistsFilterCriteria(file_path, server_type)
        if exists:
            if server_type == 'excel':
                criteria = pd.read_excel(file_path, sheet_name=table_name)
                return criteria            
            else: # server_type == 'mysql' 
                criteria = pd.read_sql("select * from " + FilterCriteria_TableName ,con=con)
                return criteria
        return None
        
    @classmethod
    def fromDict(cls, d):
        df = {k : v for k, v in d.items() if k in FilterCriteria_Columns}
        return cls(**df)


# Read Data file and Create DataFrame
class FileDataFrame(object):
    def __init__(self, file_path, delimiter):
        self.file_path = file_path
        self.delimiter = delimiter # in our case, delimiter is '\t' because data file is tab splitted file
    # read DataFrame from data file
    def readDataFrameFromFile(self):
        # open file with encoding UTF-16
        doc = codecs.open(self.file_path, 'rU', 'UTF-16')
        # read DataFrame
        self.df = pd.read_csv(doc, sep = self.delimiter)
    # Rename column names. e.g. BOS-SMART-CODE-1.10: POI_Switch = > POI_Switch, All: Robustness Index => Robustness Index
    def renameColumnsOfDataFrame(self):
        new_columns = []
        for col in self.df.columns:
            new_col = col.replace('BOS-SMART-CODE-1.10: ', '') # remove prefix 'BOS-SMART-CODE-1.10: '
            new_col = new_col.replace('All: ', '')             # remove prefix 'All: '
            new_col = new_col.replace('% ', '')                # remove punctation '%'
            new_columns.append(new_col)
        self.df.columns = new_columns
    # define get, set function
    def getDataFrame(self):
        return self.df
    def setDataFrame(self, df):
        self.df = df

# calculate All Robustness_Index
'''
    IS data:
    isPart = IS Net Profit * 365 days / number of days in IS period
    OOS data
    oosPart = OOS Net Profit * 365 days / number of days in OOS period
    RI = oosPart / isPart * 100
'''
def get_ALL_Robustness_Index(candidate, start, end, rate1=1, rate2=1):
    start = parse(Start_Date, dayfirst=True)
    end = parse(End_Date, dayfirst=True)
    # calculate delta days between two dates
    # for now IS:OS days rate is 80:20
    delta = (end - start).days
    isPart = candidate.get("IS_Net_Profit") * 365/ (delta * rate1)
    osPart = candidate.get("OS_Net_Profit") * 365/ (delta * rate2)
    RI = osPart / isPart * 100
    return RI

# calculate All:NP/DD
'''
    ALL NP = IS Net Profit + OOS Net Profit
    Max IS/OOS DD = whichever is greater: -1*(IS Max Intraday Drawdown) versus -1*(OOS Max Intraday Drawdown)
    Ratio = ALL NP / Max IS/OOS DD
'''
def get_All_NP_DD(candidate):
    All_NP = candidate.get("IS_Net_Profit") + candidate.get("OS_Net_Profit")
    Max_DD = max(abs(candidate.get("IS_Max_Intraday_Drawdown")), abs(candidate.get("OS_Max_Intraday_Drawdown")))
    Ratio = All_NP / Max_DD
    return Ratio

# calculate Avg trades per year
def get_Avg_trades_per_year(start, end, trades, rate=1):
    start = parse(start, dayfirst=True)
    end = parse(end, dayfirst=True)
    # calculate delta days between two dates
    delta = (end - start).days
    # Avg trades per year
    # for now IS:OS days rate is 80:20
    return float(trades) * 365 / (delta * rate)

# Check if candidate passes the Filter Criteria
def checkFilterCriteria(candidate, filterCriteria = FilterCriteria()):
    # 1. IS: Net Profit > IS_NP
    if candidate.get("IS_Net_Profit") <= filterCriteria.IS_NP:
        return False, 0
    # 2. OOS: Net Profit > OOS_NP
    if candidate.get("OS_Net_Profit") <= filterCriteria.OOS_NP:
        return False, 0
    # 3. OOS vs IS Avg Trade > ALL_Robustness_Index . Calculation \ Ie OOS Avg Trade should be at least 80% of the IS Avg Trade
    if candidate.get("OS_Avg_Trade") / candidate.get("IS_Avg_Trade") * 100 <= filterCriteria.ALL_Robustness_Index :
        return False, 0
    # 4. ALL: Robustness Index > ALL_Robustness_Index
    All_Robustness_Index = get_ALL_Robustness_Index(candidate, Start_Date, End_Date, IS_Percent, OOS_Percent)
    if All_Robustness_Index <= filterCriteria.ALL_Robustness_Index:
        return False, 0
    # 5. ALL: NP:DD Ratio > ALL_NP_DD_Ratio . Calculation:
    #    a. ALL: NP = IS Net Profit + OOS Net Profit
    #    b. ALL: DD = max(-IS Max Intraday Drawdown, -OOS Max Intraday Drawdown)
    #    c. Ratio = ALL: NP / ALL: DD 
    if get_All_NP_DD(candidate) <= filterCriteria.ALL_NP_DD_Ratio:
        return False, 0
    # 6. IS: Avg Trade > IS_Avg_Trade
    if candidate.get("IS_Avg_Trade") <= filterCriteria.IS_Avg_Trade:
        return False, 0
    # 7. IS: Avg trades num per year > IS_Trades_Per_Year . 
    # Ie the average number of trades per year (365 days) should be greater than 40
    if get_Avg_trades_per_year(Start_Date, End_Date, candidate.get("IS_Total_Trades"), IS_Percent) <= filterCriteria.IS_Trades_Per_Year:
        return False, 0
    # 8. OOS: Avg trades num per year > OOS_Total_Trades . 
    # Ie the average number of trades per year (365 days) should be greater than 40
    if get_Avg_trades_per_year(Start_Date, End_Date, candidate.get("OS_Total_Trades"), OOS_Percent) <= filterCriteria.OOS_Trades_Per_Year:
        return False, 0
    # 9. OOS: Total Trades > OOS_Total_Trades
    if candidate.get("OS_Total_Trades") <= filterCriteria.OOS_Total_Trades:
        return False, 0

    return True, round(Decimal(All_Robustness_Index),2)

# get Filter Criteria from file
def getFilterCriteriaFromDB(file_path, server_type):
    # create engine to load and store dataframe with mysql
    db_connection = createDBConnection(server_type)
    # load Filter Criteria from DB    
    criteria = FilterCriteria.getFilterCriteriaFromDB(file_path, FilterCriteria_TableName, server_type, db_connection)
    if criteria is None: # if Filter Criteria File doesn't exist
        # Create Default Filter Criteria
        filter_criteria = FilterCriteria()
        # get DataFrame from Default Filter Criteria
        df = pd.DataFrame.from_records([filter_criteria.to_dict()])
        filter_criteria.from_dataframe(df)
        # store Filter Criteria into DataBase
        filter_criteria.storeFilterCriteriaToDB(file_path, FilterCriteria_TableName, server_type, db_connection)

    else: # if Filter Criteria exists, read it
        # convert DataFrame to Class object
        criteria_dict = criteria.T.to_dict().values()
        for cri in criteria_dict:
            filter_criteria = FilterCriteria.fromDict(cri)
            filter_criteria.criteria = criteria
            break
    return filter_criteria

# Create DB Connection engine
def createDBConnection(server_type):
    # generate db connection string
    db_connection_str = ''
    if server_type == 'excel':
        return None
    if server_type == 'mysql':
        db_connection_str = 'mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(MySQL_User, MySQL_Password, MySQL_Host, DB_Name)
    # create db connection
    db_connection = create_engine(db_connection_str)
    return db_connection

# rename Candidate DataFrame with new column list
def renameCandidateDataFrame(dataframe, columns):
    dataframe.columns = columns


# build Candidate from IS and OS data

# from IS data

''' 
    Test
    a. POI_Switch
    b. NATR
    c. Fract
    d. Filter1_Switch
    e. Filter1_N1
    f. Filter1_N2
    g. Filter2_Switch
    h. Filter2_N1
    i. Filter2_N2
'''
'''
    a. IS: TS Index (this is the value to rank on for this task)
    b. IS: Net Profit
    c. IS: Total Trades
    d. IS: % Profitable
    e. IS: Avg Trade (this is the field to order on for second category below)
    f. IS: Max Intraday Drawdown
    g. IS: ProfitFactor (this is the field to order on for third category below)
    h. Robustness Index
'''
# from OS data
'''
    a. OOS: Net Profit
    b. OOS: Total Trades
    c. OOS: % Profitable
    d. OOS: Avg Trade (this is the field to order on for second category below)
    e. OOS: Max Intraday Drawdown
    f. OOS: ProfitFactor (this is the field to order on for third category below)
'''
def buildCandidateByTEST(IS_object, OS_object):
    # get DataFrame from IS and OS data
    IS_df = IS_object.getDataFrame()
    OS_df = OS_object.getDataFrame()

    # get needed IS and OS DataFrame by Column name list
    new_IS_df = IS_df[Candidate_Attribute_Columns + Candidate_IS_Data_Columns]
    new_OS_df = OS_df[Candidate_OS_Data_Columns]

    # rename DataFrame columns. e.g. 'IS TS Index'=>'IS_TS_Index'
    renameCandidateDataFrame(new_IS_df, New_Candidate_IS_Data_Columns)
    renameCandidateDataFrame(new_OS_df, NEW_Candidate_OS_Data_Columns)

    # get Candidates DataFrame by concatenating IS and OS DataFrame using Test key
    Candidates_df = pd.concat([new_IS_df.set_index('Test'), new_OS_df.set_index('Test')], axis=1, join='inner')
    # rounding DataFrame
    Candidates_df = Candidates_df.round(2)

    # convert value to floatString. e.g. 3.0 => "3", 1.10 => "1.1"
    Candidates_df["POI_Switch"] = Candidates_df['POI_Switch'].map('{0:g}'.format)

    # create Attributes Column from Candidate Attributes
    Candidates_df["Attributes"] = Candidates_df.apply(lambda row: "".join([str(row.POI_Switch), str(row.NATR), str(row.Fract), str(row.Filter1_Switch), \
        str(row.Filter1_N1), str(row.Filter1_N2), str(row.Filter2_Switch), str(row.Filter2_N1), str(row.Filter2_N2)]), axis=1)
    
    Candidates_df.insert(0, column ='Test', value = Candidates_df.index)
    Candidates_df = Candidates_df.reset_index(drop=True)

    #Sort by 'IS TS Index' descending order, 'Test' ascending order
    Candidates_df = Candidates_df.sort_values(['IS_TS_Index', 'Test'], ascending=[False, True], ignore_index=True)
    # store Candidates into file
    return Candidates_df

# pass Filter Criteria
def passFilterCriteria(df, criteria):
    # convert DataFrame into Dictionary list
    df_dict = df.T.to_dict().values()    
    # Candidates list which passes Filter Criteria
    passed_candidates = []
    # All Robustness Index list which passed Filter Critria
    All_Robustness_Index_List = []
    for row in df_dict: # loop Dictionary list
        # check if Candidate passes Filter Criteria
        # isPassFilterCriteria, All_Robustness_Index = ob.checkFilterCriteria(criteria)
        isPassFilterCriteria, All_Robustness_Index = checkFilterCriteria(row)
        if isPassFilterCriteria: # if it passes, append it
            passed_candidates.append(row)
            All_Robustness_Index_List.append(All_Robustness_Index)
    # convert Dictionary list into DataFrame
    passed_df = pd.DataFrame(passed_candidates)
    passed_df['All_Robustness_Index'] = All_Robustness_Index_List

    # check Duplicity < 90
    passed_df = passed_df.query("Duplicity<90").reset_index(drop=True)

    # sort passed DataFrame by IS TS Index descending order
    # and get top 30 candidates
    IS_TS_Index_df = passed_df.sort_values(['IS_TS_Index'], ascending=False, ignore_index=True).head(30)

    # sort passed DataFrame by IS Avg Trade descending order
    # and get top 30 candidates
    IS_Avg_Trade_df = passed_df.sort_values(['IS_Avg_Trade'], ascending=False, ignore_index=True).head(30)

    # sort passed DataFrame by IS Profitable descending order
    # and get top 30 candidates
    IS_ProfitFactor_df = passed_df.sort_values(['IS_ProfitFactor'], ascending=False, ignore_index=True).head(30)

    # Build final single unique candidate list
    # concatenate three dataframes into one dataframe by remove duplicated candidates
    passed_df = pd.concat([IS_TS_Index_df, IS_Avg_Trade_df, IS_ProfitFactor_df]).drop_duplicates().reset_index(drop=True)

    return passed_df
# pass duplicity
def calcDuplicity(df, criteria):
    # loop dataframe
    # convert DataFrame into Dictionary list
    df_dict = df.T.to_dict().values()
    df_list = list(df_dict)
    index_list = range(len(df_list))

    # Create 4 Processing 
    pool = Pool(4)
    duplicity_list = pool.map(partial(processDuplicity, df_list), index_list) # Returns a list

    # Add Duplicity Column in DataFrame
    df.insert(loc=1, column ='Duplicity', value= duplicity_list)
    df = df.drop(columns='Attributes')
    return df

# multiprocessing for calculating Duplicity
def processDuplicity(df_list, i):
    # current row
    cur = df_list[i]
    # get start and end position from current position
    if i < 1:
        return 0
    # calc boundary of current Candidate, i.e. start position and end position
    start_p = max(i - Duplicity_Candidate_Count, 0)
    end_p = i - 1

    k = start_p 
    max_duplicity = 0 # max duplicity value
    while k <= end_p: # loop from start to end position of Candidates

        # calcuate Duplicity with kth Candidate
        duplicity = calcDuplicity_TwoCandidates(cur, df_list[k])
        # update max duplicity
        if duplicity > max_duplicity:
            max_duplicity = duplicity
        k += 1
    return max_duplicity

def calcDuplicity_TwoCandidates(cur, other):
    # calculate IDV
    # 100 - difference between attributes of two candidates
    IDV = 100 - compare_string2(cur.get("Attributes"), other.get("Attributes"), 15)
    
    # calculate NPR
    # initial NPR is 1.0
    # NPR = abs(NP1/NP2), if NPR >1, then NPR = 1/NPR

    NPR = 1
    if cur.get("IS_Net_Profit") != 0 and other.get("IS_Net_Profit") != 0:
        NPR = abs(cur.get("IS_Net_Profit")/other.get("IS_Net_Profit"))
        if NPR > 1:
            NPR = 1.0 / NPR

    # calculate TTR
    # TTR uses the same formula as NPR    
    TTR = 1
    if cur.get("IS_Total_Trades") != 0 and other.get("IS_Total_Trades") != 0:
        TTR = abs(cur.get("IS_Total_Trades")/other.get("IS_Total_Trades"))
        if TTR > 1:
            TTR = 1.0 / TTR
    return int(IDV * NPR * TTR)

# compare two string from Candidate Attributes  "5250.9114104124", "3501.6534161629204"
# and get the count of common charactors.
def compare_string2(s1, s2, maxOffset=15):
    diff = 0
    if not (s1 and s1.strip()):
        if not s2:
            diff = 0
        else:
            diff = len(s2)        
    elif not (s2 and s2.strip()):
        diff = len(s1)
    else:
        index = 0
        offset1 = 0
        offset2 = 0
        count = 0
        while ((index + offset1) < len(s1) and (index + offset2) < len(s2)):
            if s1[index + offset1] == s2[index + offset2]:
                count += 1
            else:
                offset1 = 0
                offset2 = 0
                offset = 0
                while offset <= maxOffset - 1:
                    if (index + offset) < len(s1) and s1[index + offset] == s2[index]:
                        offset1 = offset
                        break
                    if (index + offset) < len(s2) and s1[index] == s2[index + offset]:
                        offset2 = offset
                        break

                    offset += 1
            index += 1
        
        diff = float(len(s1) + len(s2)) / 2 - count
    return diff

# Create DataBase for mysql
def createDataBase(server_type):
    cnx = connectDataBase(server_type)
    if cnx is None:
        global Server_Type
        Server_Type = "excel"
        return
    if server_type == 'mysql':
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

# connect DataBase of MySQL
def connectDataBase(server_type, db_name=""):
    cnx = None
    if server_type == 'mysql':
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
            print("Candidates will be stored in Excel")
    return cnx 
# check if Filter Criteria table exists in file or database of MySQL
def checkExistsFilterCriteria(file_path, server_type):
    if server_type == 'excel':
        return os.path.exists(file_path)
    else: # server_type == 'mysql' 
        conn = connectDataBase(server_type, db_name=DB_Name)
        if server_type == 'mysql':
            cur = conn.cursor(buffered=True)
        else:
            cur = conn.cursor()
        cur.execute("select * from information_schema.tables where table_name=%s", (FilterCriteria_TableName,))
        exists = cur.rowcount
        cur.close()
        conn.close()
        return bool(exists)
        

# store DataFrame in excel or MySQL
def storeDataFrameInDB(file_path, dataframe, table_name, server_type):
    db_connection = createDBConnection(server_type)
    if server_type == 'excel':
        dataframe.to_excel(file_path, sheet_name=table_name)
    else: # server_type == 'mysql'
        dataframe.to_sql(table_name, db_connection, if_exists='replace')

if __name__ == "__main__":
    # for multiprocessing
    freeze_support()
    # create DataBase
    if Server_Type == 'mysql':
        createDataBase(Server_Type)

    # Load Filter Criteria
    filter_criteria = getFilterCriteriaFromDB('FilterCriteria.xlsx', Server_Type)

    # Read IS and OOS file
    IS_object = FileDataFrame(IS_Result_File, '\t')
    OS_object = FileDataFrame(OOS_Result_File, '\t')
    IS_object.readDataFrameFromFile()
    IS_object.renameColumnsOfDataFrame()
    OS_object.readDataFrameFromFile()
    OS_object.renameColumnsOfDataFrame()
    # Merge IS and OS by Test index
    Candidates_df = buildCandidateByTEST(IS_object, OS_object)

    # Calculate Duplicity
    Duplicity_df = calcDuplicity(Candidates_df, filter_criteria)
    # storeDataFrameInDB('calcDuplicity.xlsx', Duplicity_df, 'duplicity', Server_Type)

    # Pass Filter Criteria
    Passed_df = passFilterCriteria(Duplicity_df, filter_criteria)
    # Store passed candidates into DataBase
    storeDataFrameInDB('passed_candidates.xlsx',  Passed_df, Candidates_TableName, Server_Type)


