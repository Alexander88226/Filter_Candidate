# Load Libraries
import os, sys
import math
import pandas as pd
import codecs
from dateutil.parser import parse
from datetime import date

if len(sys.argv) != 3:
    exit(0)
Start_Date = sys.argv[1]
End_Date = sys.argv[2]

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
    def __init__(self, Start_Date=Start_Date, End_date=End_Date, IS_NP = 0, OOS_NP = 0, OOS_IS_Avg_Trade=80, 
                 ALL_Robustness_Index = 80, ALL_NP_DD_Ratio = 2, IS_Avg_Trade = 60, IS_Trades_Per_Year = 40,
                OOS_Trades_Per_Year = 40, OOS_Total_Trades = 10, Duplicity= 90):
        print(Start_Date)
        self.Start_Date             = Start_Date
        self.End_date               = End_date
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
            'Start_Date'            : self.Start_Date,
            'End_date'              : self.End_date,
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

    # get filter criteria from Excel file
    @classmethod
    def  getFilterCriteriaFromExcel(cls, file_path, sheet_name="FilterCriteria"):
        if os.path.exists(file_path):
            criteria = pd.read_excel(file_path, sheet_name=sheet_name)
            return criteria
            
        else:
            print("Filter Criteria file doesn't exist")
            return None
    # store filter criteria into Excel file
    def storeFilterCriteriaToExcel(self, file_path, sheet_name="FilterCriteria"):
        self.criteria.to_excel(file_path, sheet_name=sheet_name)
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
        # self.IS_Start_Date             = Start_Date
        # self.IS_End_Date               = End_Date
    def to_dict(self):
        return {
            'Test'                  : self.Test,
            'IS_TS_Index'              : self.IS_TS_Index,
            'IS_Net_Profit'            : self.IS_Net_Profit,
            'IS_Total_Trades'          : self.IS_Total_Trades,
            'IS_Profitable'            : self.IS_Profitable,
            'IS_Avg_Trade'             : self.IS_Avg_Trade,
            'IS_Max_Intraday_Drawdown' : self.IS_Max_Intraday_Drawdown,
            'IS_ProfitFactor'          : self.IS_ProfitFactor,
            'IS_Robustness_Index'      : self.IS_Robustness_Index,
            # 'IS_Start_Date'            : self.IS_Start_Date,
            # 'IS_End_Date'              : self.IS_End_Date,
        }
    def calcDuplicity(self):
        print(self.Test)


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
        # self.OS_Start_Date             = Start_Date
        # self.OS_End_Date               = End_Date

    def to_dict(self):
        return {
            'OS_Net_Profit'            : self.OS_Net_Profit,
            'OS_Total_Trades'          : self.OS_Total_Trades,
            'OS_Profitable'            : self.OS_Profitable,
            'OS_Avg_Trade'             : self.OS_Avg_Trade,
            'OS_Max_Intraday_Drawdown' : self.OS_Max_Intraday_Drawdown,
            'OS_ProfitFactor'          : self.OS_ProfitFactor,
            'OS_Start_Date'            : self.OS_Start_Date,
            'OS_End_Date'              : self.OS_End_Date,
            'OS_Robustness_Index'      : self.OS_Robustness_Index,
        }

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
            return False
        # 2. OOS: Net Profit > OOS_NP
        if self.OS_Net_Profit <= filterCriteria.OOS_NP:
            return False
        # 3. OOS vs IS Avg Trade > ALL_Robustness_Index . Calculation \ Ie OOS Avg Trade should be at least 80% of the IS Avg Trade
        if float(self.OS_Avg_Trade) / float(self.IS_Avg_Trade) * 100<= filterCriteria.ALL_Robustness_Index :
            return False
        # 4. ALL: Robustness Index > ALL_Robustness_Index
        if self.get_ALL_Robustness_Index(self.Start_Date, self.End_Date, 0.8, 0.2) <= filterCriteria.ALL_Robustness_Index:
            return False
        # 5. ALL: NP:DD Ratio > ALL_NP_DD_Ratio . Calculation:
        #    a. ALL: NP = IS Net Profit + OOS Net Profit
        #    b. ALL: DD = IS Max Intraday Drawdown + OOS Max Intraday Drawdown
        #    c. Ratio = ALL: NP / ALL: DD 
        if self.get_All_NP_DD() <= filterCriteria.ALL_NP_DD_Ratio:
            return False
        # 6. IS: Avg Trade > IS_Avg_Trade
        if self.IS_Avg_Trade <= filterCriteria.IS_Avg_Trade:
            return False
        # 7. IS: Avg trades num per year > IS_Trades_Per_Year . 
        # Ie the average number of trades per year (365 days) should be greater than 40
        if self.get_Avg_trades_per_year(self.Start_Date, self.End_Date, self.IS_Total_Trades, 0.8) <= filterCriteria.IS_Trades_Per_Year:
            return False
        # 8. OOS: Avg trades num per year > OOS_Total_Trades . 
        # Ie the average number of trades per year (365 days) should be greater than 40
        if self.get_Avg_trades_per_year(self.Start_Date, self.End_Date, self.OS_Avg_Trade, 0.2) <= filterCriteria.OOS_Trades_Per_Year:
            return False
        # 9. OOS: Total Trades > OOS_Total_Trades
        if self.OS_Total_Trades <= filterCriteria.OOS_Total_Trades:
            return False
        # 10. Duplicity < 95. See below for duplicity calculation

        return True
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
    def get_All_NP_DD(self):
        All_NP = self.IS_Net_Profit + self.OS_Net_Profit
        Max_DD = max(abs(self.IS_Max_Intraday_Drawdown), abs(self.OS_Max_Intraday_Drawdown))
        Ratio = float(All_NP) / Max_DD
        return Ratio

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
def getFilterCriteriaFromFile(file_path):
    # load Filter Criteria from file
    criteria = FilterCriteria.getFilterCriteriaFromExcel(file_path, 'FilterCriteria')
    if criteria is None: # if Filter Criteria File doesn't exist
        # Create Default Filter Criteria
        filter_criteria = FilterCriteria()
        # get DataFrame from Default Filter Criteria
        df = pd.DataFrame.from_records([filter_criteria.to_dict()])
        filter_criteria.from_dataframe(df)
        # store Filter Criteria into Excel file
        filter_criteria.storeFilterCriteriaToExcel(file_path, 'FilterCriteria')

    else: # if Filter Criteria exists, read it
        # convert DataFrame to Class object
        criteria_dict = criteria.T.to_dict().values()
        for cri in criteria_dict:
            filter_criteria = FilterCriteria.fromDict(cri)
            filter_criteria.criteria = criteria
            break
    return filter_criteria



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
    Candidates_df['Test'] = Candidates_df.index
    # store Candidates into file
    Candidates_df.to_excel('candidates.xlsx', sheet_name='candidates')
    return Candidates_df

# pass Filter Criteria
def passFilterCriteria(df, criteria):
    # convert DataFrame into Dictionary list
    df_dict = df.T.to_dict().values()    
    # Candidates list which passes Filter Criteria
    passed_candidates = []
    for row in df_dict: # loop Dictionary list
        # get Candidate object from Dictionary item
        ob = Candidate.fromDict(row)
        # check if Candidate passes Filter Criteria
        if ob.checkFilterCriteria(criteria): # if it passes, append it
            passed_candidates.append(row)
    # convert Dictionary list into DataFrame
    passed_df = pd.DataFrame(passed_candidates)
    # sort passed DataFrame by IS TS Index descending order
    passed_df = passed_df.sort_values(['IS_TS_Index'], ascending=False, ignore_index=True)
    # Candidates_df.to_excel('PassedCandidates.xlsx', sheet_name='candidates')
    return passed_df
def passDulicity(df, criteria):
    pass
if __name__ == "__main__":


    filter_criteria = getFilterCriteriaFromFile('FilterCriteria.xlsx')
    IS_object = FileDataFrame('IS.txt', '\t')
    OS_object = FileDataFrame('OOS.txt', '\t')
    IS_object.readDataFrameFromFile()
    IS_object.renameColumnsOfDataFrame()
    OS_object.readDataFrameFromFile()
    OS_object.renameColumnsOfDataFrame()
    Candidates_df = buildCandidateByTEST(IS_object, OS_object)
    Passed_df = passFilterCriteria(Candidates_df, filter_criteria)
    Passed_df.to_excel('PassedCandidates.xlsx', sheet_name='candidates')


"FilterCriteria.xlsx"


