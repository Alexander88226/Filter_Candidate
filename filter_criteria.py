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


# check arguments length
if len(sys.argv) != 4:
    exit(0)
# first argv is Start Date
# second one is End Date
# third one is Server Type : excel, mysql, postgre
Start_Date = sys.argv[1]
End_Date = sys.argv[2]
Server_Type = sys.argv[3].lower()
# check Server Type
if Server_Type != 'excel' and Server_Type != 'mysql' and Server_Type != 'postgre':
    print("Please input correct Server Type as one of excel, mysql, postgre")
    exit(0)

OOS_Percent = 0.2
IS_Percent = 1 - OOS_Percent
Duplicity_Candidate_Count = 100



# DataBase Name
DB_Name = 'candidatesdb'
# Table Name
FilterCriteria_TableName = "filtercriteria"
Candidates_TableName = "candidates"

# MySQL configuration
MySQL_User = 'user'
MySQL_Password = 'password'
MySQL_Host = '127.0.0.1'

# PostgreSQL configuration
PostgreSQL_User = 'user'
PostgreSQL_Password = 'password'
PostgreSQL_Host = '127.0.0.1'

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
    def storeFilterCriteriaToDB(self, file_path, table_name="filtercriteria", server_type='excel', con=None):
        if server_type == 'excel':
            self.criteria.to_excel(file_path, sheet_name=table_name)
        else: # server_type == 'mysql' or server_type == 'postgre':
            self.criteria.to_sql(table_name, con, if_exists='replace')
    
    # get filter criteria from Excel file
    @classmethod
    def getFilterCriteriaFromDB(cls, file_path, table_name="filtercriteria", server_type='excel', con=None):
        exists = checkExistsFilterCriteria(file_path, server_type)
        if exists:
            if server_type == 'excel':
                criteria = pd.read_excel(file_path, sheet_name=table_name)
                return criteria            
            else: # server_type == 'mysql' or server_type == 'postgre':
                criteria = pd.read_sql("select * from " + FilterCriteria_TableName ,con=con)
                return criteria
        return None
        
    @classmethod
    def fromDict(cls, d):
        df = {k : v for k, v in d.items() if k in FilterCriteria_Columns}
        return cls(**df)

# Class of Candidate Value from IS Data
class Candidate_IS_Data(object):
    def __init__(self, Test, TS_Index, Net_Profit, Total_Trades, Profitable, Avg_Trade, Max_Intraday_Drawdown, ProfitFactor, Robustness_Index):
        self.Test                      = Test
        self.IS_TS_Index               = TS_Index
        self.IS_Net_Profit             = Net_Profit
        self.IS_Total_Trades           = Total_Trades
        self.IS_Profitable             = Profitable
        self.IS_Avg_Trade              = Avg_Trade
        self.IS_Max_Intraday_Drawdown  = Max_Intraday_Drawdown
        self.IS_ProfitFactor           = ProfitFactor
        self.IS_Robustness_Index       = Robustness_Index
    # calculate NPR
    def calcNPR(self, other):
        # 1 - (Abs(NP1 - NP2) / NP1)
        # return 1 - float(abs(self.IS_Net_Profit - other.IS_Net_Profit)) / self.IS_Net_Profit

        # initial NPR is 1.0
        # NPR = abs(NP1/NP2), if NPR >1, then NPR = 1/NPR
        NPR = 1
        if self.IS_Net_Profit != 0 and other.IS_Net_Profit != 0:
            NPR = abs(float(self.IS_Net_Profit)/other.IS_Net_Profit)
            if NPR > 1:
                NPR = 1.0 / NPR
        return NPR
    
    # calculate TTR
    def calcTTR(self, other):
        # 1 - (Abs(TT1 - TT2) / TT1)
        # return 1 - float(abs(self.IS_Total_Trades - other.IS_Total_Trades)) / self.IS_Total_Trades
        # TTR uses the same formula as NPR
        TTR = 1
        if self.IS_Total_Trades != 0 and other.IS_Total_Trades != 0:
            TTR = abs(float(self.IS_Total_Trades)/other.IS_Total_Trades)
            if TTR > 1:
                TTR = 1.0 / TTR
        return TTR

# Class of Candidate Value from OS Data
class Candidate_OS_Data(object):
    def __init__(self, Net_Profit, Total_Trades, Profitable, Avg_Trade, Max_Intraday_Drawdown, ProfitFactor, Robustness_Index):
        self.OS_Net_Profit             = Net_Profit
        self.OS_Total_Trades           = Total_Trades
        self.OS_Profitable             = Profitable
        self.OS_Avg_Trade              = Avg_Trade
        self.OS_Max_Intraday_Drawdown  = Max_Intraday_Drawdown
        self.OS_ProfitFactor           = ProfitFactor
        self.OS_Robustness_Index       = Robustness_Index

# Class of Candidate attributes from IS and OS Data
class Candidate_Attribute(object):
    def __init__(self, POI_Switch, NATR, Fract, Filter1_Switch, Filter1_N1, Filter1_N2, Filter2_Switch, Filter2_N1, Filter2_N2):
        self.POI_Switch     = POI_Switch
        self.NATR           = NATR
        self.Fract          = Fract
        self.Filter1_Switch = Filter1_Switch
        self.Filter1_N1     = Filter1_N1
        self.Filter1_N2     = Filter1_N2
        self.Filter2_Switch = Filter2_Switch
        self.Filter2_N1     = Filter2_N1
        self.Filter2_N2     = Filter2_N2
        self.Start_Date     = Start_Date
        self.End_Date       = End_Date
    def to_dict(self):
        return {
            'POI_Switch'        : self.POI_Switch,
            'NATR'              : self.NATR,
            'Fract'             : self.Fract,
            'Filter1_Switch'    : self.Filter1_Switch,
            'Filter1_N1'        : self.Filter1_N1,
            'Filter1_N2'        : self.Filter1_N2,
            'Filter2_Switch'    : self.Filter2_Switch,
            'Filter2_N1'        : self.Filter2_N1,
            'Filter2_N2'        : self.Filter2_N2,
            'Start_Date'        : self.Start_Date,
            'End_Date'          : self.End_Date,
        }
    # calculate IDV
    def calcIDV(self, other):
        # generate number's string from IS and OS attributes
        self_s = ""
        self_s += convertFloatString(self.POI_Switch)
        self_s += convertFloatString(self.NATR)
        self_s += convertFloatString(self.Fract)
        self_s += convertFloatString(self.Filter1_Switch)
        self_s += convertFloatString(self.Filter1_N1)
        self_s += convertFloatString(self.Filter1_N2)
        self_s += convertFloatString(self.Filter2_Switch)
        self_s += convertFloatString(self.Filter2_N1)
        self_s += convertFloatString(self.Filter2_N2)

        other_s = ""
        other_s += convertFloatString(other.POI_Switch)
        other_s += convertFloatString(other.NATR)
        other_s += convertFloatString(other.Fract)
        other_s += convertFloatString(other.Filter1_Switch)
        other_s += convertFloatString(other.Filter1_N1)
        other_s += convertFloatString(other.Filter1_N2)
        other_s += convertFloatString(other.Filter2_Switch)
        other_s += convertFloatString(other.Filter2_N1)
        other_s += convertFloatString(other.Filter2_N2)

        diff = compare_string2(self_s, other_s, 15)
        return 100 - diff


# Class Candidate from IS and OS
class Candidate(Candidate_Attribute, Candidate_IS_Data, Candidate_OS_Data):
    def __init__(self, Test, POI_Switch, NATR, Fract, Filter1_Switch, Filter1_N1, Filter1_N2, Filter2_Switch, Filter2_N1, Filter2_N2,
                IS_TS_Index, IS_Net_Profit, IS_Total_Trades, IS_Profitable, IS_Avg_Trade, IS_Max_Intraday_Drawdown, IS_ProfitFactor, IS_Robustness_Index, 
                OS_Net_Profit, OS_Total_Trades, OS_Profitable, OS_Avg_Trade, OS_Max_Intraday_Drawdown, OS_ProfitFactor, OS_Robustness_Index):
        Candidate_Attribute.__init__(self, POI_Switch, NATR, Fract, Filter1_Switch, Filter1_N1, Filter1_N2, Filter2_Switch, Filter2_N1, Filter2_N2)
        Candidate_IS_Data.__init__(self,Test, IS_TS_Index, IS_Net_Profit, IS_Total_Trades, IS_Profitable, IS_Avg_Trade, IS_Max_Intraday_Drawdown, IS_ProfitFactor, IS_Robustness_Index)
        Candidate_OS_Data.__init__(self,OS_Net_Profit, OS_Total_Trades, OS_Profitable, OS_Avg_Trade, OS_Max_Intraday_Drawdown, OS_ProfitFactor, OS_Robustness_Index)
    # Check if candidate passes the Filter Criteria
    def checkFilterCriteria(self, filterCriteria = FilterCriteria()):
        # 1. IS: Net Profit > IS_NP
        if self.IS_Net_Profit <= filterCriteria.IS_NP:  
            return False, 0
        # 2. OOS: Net Profit > OOS_NP
        if self.OS_Net_Profit <= filterCriteria.OOS_NP:
            return False, 0
        # 3. OOS vs IS Avg Trade > ALL_Robustness_Index . Calculation \ Ie OOS Avg Trade should be at least 80% of the IS Avg Trade
        if float(self.OS_Avg_Trade) / float(self.IS_Avg_Trade) * 100<= filterCriteria.ALL_Robustness_Index :
            return False, 0
        # 4. ALL: Robustness Index > ALL_Robustness_Index
        All_Robustness_Index = self.get_ALL_Robustness_Index(self.Start_Date, self.End_Date, IS_Percent, OOS_Percent)
        if All_Robustness_Index <= filterCriteria.ALL_Robustness_Index:
            return False, 0
        # 5. ALL: NP:DD Ratio > ALL_NP_DD_Ratio . Calculation:
        #    a. ALL: NP = IS Net Profit + OOS Net Profit
        #    b. ALL: DD = max(-IS Max Intraday Drawdown, -OOS Max Intraday Drawdown)
        #    c. Ratio = ALL: NP / ALL: DD 
        if self.get_All_NP_DD() <= filterCriteria.ALL_NP_DD_Ratio:
            return False, 0
        # 6. IS: Avg Trade > IS_Avg_Trade
        if self.IS_Avg_Trade <= filterCriteria.IS_Avg_Trade:
            return False, 0
        # 7. IS: Avg trades num per year > IS_Trades_Per_Year . 
        # Ie the average number of trades per year (365 days) should be greater than 40
        if self.get_Avg_trades_per_year(self.Start_Date, self.End_Date, self.IS_Total_Trades, IS_Percent) <= filterCriteria.IS_Trades_Per_Year:
            return False, 0
        # 8. OOS: Avg trades num per year > OOS_Total_Trades . 
        # Ie the average number of trades per year (365 days) should be greater than 40
        if self.get_Avg_trades_per_year(self.Start_Date, self.End_Date, self.OS_Total_Trades, OOS_Percent) <= filterCriteria.OOS_Trades_Per_Year:
            return False, 0
        # 9. OOS: Total Trades > OOS_Total_Trades
        if self.OS_Total_Trades <= filterCriteria.OOS_Total_Trades:
            return False, 0

        return True, round(Decimal(All_Robustness_Index),2)
    # get Class object from Dictionary
    @classmethod
    def fromDict(cls, d):
        df = {k : v for k, v in d.items() if k in Candidate_Columns}
        return cls(**df)
    
    # calculate Avg trades per year
    def get_Avg_trades_per_year(self, start, end, trades, rate=1):
        start = parse(self.Start_Date, dayfirst=True)
        end = parse(self.End_Date, dayfirst=True)
        # calculate delta days between two dates
        delta = (end - start).days
        # Avg trades per year
        # for now IS:OS days rate is 80:20
        return float(trades) * 365 / (float(delta) * rate)

    # calculate All Robustness_Index
    '''
        IS data:
        isPart = IS Net Profit * 365 days / number of days in IS period
        OOS data
        oosPart = OOS Net Profit * 365 days / number of days in OOS period
        RI = oosPart / isPart * 100
    '''
    def get_ALL_Robustness_Index(self, start, end, rate1=1, rate2=1):
        start = parse(self.Start_Date, dayfirst=True)
        end = parse(self.End_Date, dayfirst=True)
        # calculate delta days between two dates
        # for now IS:OS days rate is 80:20
        delta = (end - start).days
        isPart = float(self.IS_Net_Profit) * 365/ float(delta * rate1)
        osPart = float(self.OS_Net_Profit) * 365/ float(delta * rate2)
        RI = float(osPart) / isPart * 100
        return RI

    # calculate All:NP/DD
    '''
        ALL NP = IS Net Profit + OOS Net Profit
        Max IS/OOS DD = whichever is greater: -1*(IS Max Intraday Drawdown) versus  -1*(OOS Max Intraday Drawdown)
        Ratio = ALL NP / Max IS/OOS DD
    '''
    def get_All_NP_DD(self):
        All_NP = self.IS_Net_Profit + self.OS_Net_Profit
        Max_DD = max(abs(self.IS_Max_Intraday_Drawdown), abs(self.OS_Max_Intraday_Drawdown))
        Ratio = float(All_NP) / Max_DD
        return Ratio
    # Duplicity = ROUND(IDV * NPR * TTR) 
    def calcDuplicity(self, other):
        IDV = self.calcIDV(other)
        NPR = self.calcNPR(other)
        TTR = self.calcTTR(other)
        return int(IDV * NPR * TTR)



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

# get Filter Criteria from file
def getFilterCriteriaFromDB(file_path, server_type):
    # create engine to load and store dataframe with mysql and postgresql
    db_connection = createDBConnection(server_type)
    # load Filter Criteria from DB    
    criteria = FilterCriteria.getFilterCriteriaFromDB(file_path, FilterCriteria_TableName, server_type, db_connection)
    if criteria is None: # if Filter Criteria File doesn't exist
        # Create Default Filter Criteria
        filter_criteria = FilterCriteria()
        # get DataFrame from Default Filter Criteria
        df = pd.DataFrame.from_records([filter_criteria.to_dict()])
        filter_criteria.from_dataframe(df)
        # store Filter Criteria into Excel file
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
    if server_type == 'postgre':
        db_connection_str = 'postgresql://{0}:{1}@{2}/{3}'.format(PostgreSQL_User, PostgreSQL_Password, PostgreSQL_Host, DB_Name)
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
    Candidates_df = Candidates_df.round(2)
    Candidates_df.insert(0, column ='Test', value = Candidates_df.index)
    Candidates_df = Candidates_df.reset_index(drop=True)
    #, 'IS_Avg_Trade', 'IS_Profitable'
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
        # get Candidate object from Dictionary item
        ob = Candidate.fromDict(row)
        # check if Candidate passes Filter Criteria
        isPassFilterCriteria, All_Robustness_Index = ob.checkFilterCriteria(criteria)
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
    IS_Profitable_df = passed_df.sort_values(['IS_Profitable'], ascending=False, ignore_index=True).head(30)

    # Build final single unique candidate list
    # concatenate three dataframes into one dataframe by remove duplicated candidates
    passed_df = pd.concat([IS_TS_Index_df, IS_Avg_Trade_df, IS_Profitable_df]).drop_duplicates().reset_index(drop=True)

    return passed_df
# pass duplicity
def calcDulicity(df, criteria):
    # loop dataframe
    # convert DataFrame into Dictionary list
    df_dict = df.T.to_dict().values()
    df_list = list(df_dict)
    duplicity_list = []
    for i, row in enumerate(df_list): # loop Dictionary list
        # get Candidate object from Dictionary item
        ob = Candidate.fromDict(row)
        # get start and end position from current position
        if i < 1:
            duplicity_list.append(0)
            continue
        start_p, end_p = getBoundaryOfCandidate(i, len(df_list))
        if i > 150:
            end = 1
        k = start_p 
        max_duplicity = 0 # max duplicity value
        while k <= end_p: # loop from start to end position of Candidates
            # if k != i: # shouldn't compare with self
            # convert dict_values to list and get kth list
            # get Candidate Class object
            other = Candidate.fromDict(df_list[k])
            # calcuate Duplicity
            duplicity = ob.calcDuplicity(other)
            # update max duplicity
            if duplicity > max_duplicity:
                max_duplicity = duplicity
            k += 1
        duplicity_list.append(max_duplicity)
    # Add Duplicity Column in DataFrame
    df.insert(loc=1, column ='Duplicity', value= duplicity_list)
    return df

# calc boundary of current Candidate, i.e. start position and end position
def getBoundaryOfCandidate(index, length):
    start = max(index - Duplicity_Candidate_Count, 0)
    end = index - 1
    return start, end

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

# Create DataBase for mysql and postgresql
def createDataBase(server_type):
    cnx = connectDataBase(server_type)
    if server_type == 'mysql':
        try:
            cursor = cnx.cursor()
            # create database if not exists
            cursor.execute('CREATE DATABASE IF NOT EXISTS ' + DB_Name)
            cnx.commit()
            cursor.close()
        except mysqlconnector.Error as err:
            print(err)
        finally:
            if cnx is not None:
                cnx.close()
    if server_type == 'postgre':
        try:
            cnx.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = cnx.cursor()
            # check if database exists
            cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = '" + DB_Name+"'")
            exists = cursor.fetchone()
            if not exists:
                cursor.execute('CREATE DATABASE '+DB_Name)
                cnx.commit()
            cursor.close()
        except DatabaseError as error:
            print(error)
        finally:
            if cnx is not None:
                cnx.close()

# connect Database of MySQL and PostgreSQL
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
    if server_type == 'postgre':
        config = {
        'user': PostgreSQL_User,
        'password': PostgreSQL_Password,
        'host': PostgreSQL_Host,
        }
        if db_name:
            config['dbname'] = db_name

        try:
            cnx = connect(**config)
            cnx.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        except DatabaseError as error:
            print(error)     
    return cnx 

# check if Filter Criteria table exists in file or database of MySQL and PostgreSQL
def checkExistsFilterCriteria(file_path, server_type):
    if server_type == 'excel':
        return os.path.exists(file_path)
    else: # server_type == 'mysql' or server_type == 'postgre':
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
        


# convert value to floatString. e.g. 3.0 => "3", 1.10 => "1.1"
def convertFloatString(value):
    return ('%f' % value).rstrip('0').rstrip('.')

# store DataFrame in excel or MySQL or PostgreSQL
def storeDataFrameInDB(file_path, dataframe, table_name, server_type):
    db_connection = createDBConnection(server_type)
    if server_type == 'excel':
        dataframe.to_excel(file_path, sheet_name=table_name)
    else: # server_type == 'mysql' or server_type == 'postgre':
        dataframe.to_sql(table_name, db_connection, if_exists='replace')

if __name__ == "__main__":

    # create DataBase
    if Server_Type == 'mysql' or Server_Type == 'postgre':
        createDataBase(Server_Type)

    # Load Filter Criteria
    filter_criteria = getFilterCriteriaFromDB('FilterCriteria.xlsx', Server_Type)

    # Read IS and OOS file
    IS_object = FileDataFrame('IS.txt', '\t')
    OS_object = FileDataFrame('OOS.txt', '\t')
    IS_object.readDataFrameFromFile()
    IS_object.renameColumnsOfDataFrame()
    OS_object.readDataFrameFromFile()
    OS_object.renameColumnsOfDataFrame()

    # Merge IS and OS by Test index
    Candidates_df = buildCandidateByTEST(IS_object, OS_object)

    # Calculate Duplicity
    Duplicity_df = calcDulicity(Candidates_df, filter_criteria)
    # storeDataFrameInDB('calcDuplicity.xlsx', Duplicity_df, 'duplicity', Server_Type)
    
    # Pass Filter Criteria
    Passed_df = passFilterCriteria(Duplicity_df, filter_criteria)
    # Store passed candidates into DataBase
    storeDataFrameInDB('PassedCandidates.xlsx',  Passed_df, Candidates_TableName, Server_Type)


