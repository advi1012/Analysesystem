import numpy as np
import pandas as pd


#Algorithmus


import numpy as np
import pandas as pd
import datetime as datetime
from pandas.api.types import is_datetime64_any_dtype as is_datetime
from pandas.api.types import is_categorical_dtype as is_categorical

df = pd.read_csv('store_data_erweitert_Euro.csv', sep=';')
#df = pd.read_csv('fz28_2021_04_edited.csv', sep=';')
column_names = df.columns.tolist()

#print(df["Assortment"].dtype == pd.CategoricalDtype)



"""
 First collect data with list. Eather List-of-Lists or lists-of-dict. Create DataFrame when ready. Due to computationally intensitivity.
 Aufbereitung für dc.js: Welche Daten an welche Achse packen (dimension,group): 
 Kategoriale Daten erkennen, die später als Dimension in dc.js verwendet werden können. Charts nutzen die Dimension für das Filtern
 Nummerische Daten, die sich aggregieren lassen werden innerhalb der group Methode verwendet für die eine besimmte Aggregierung gewählt wird. 
 Aus der Gruppe werden die Daten gelesen und dargestellt
 

"""
dataListDatatype = []
dataList_is_numeric = []
dataList_is_relativeNumber = []
dataList_is_date_regex = []
dataList_is_date_pandas = []
dataList_is_category = []

column_names_metadata=['Datatype', 'is_numeric', 'is_relativeNumber','is_date', 'is_categoricalColumn']



# Check Categorical data. Assumption: Dates (can be displayed as dimension in charts) aswell as alphanumerical data.
# In Pandas Report: State, PromoInterval, Promo/2, Assortment, StoreType, SchoolHoliday, Open, Date


# result is the opposit of is_numeric
def is_CategorialColumn(df, column_names, dataList_is_category):
    for idx, item in enumerate(column_names):
        #print(df["Open"].dtype.name == "category") => All False
        if(df[item].dtype == pd.CategoricalDtype):
            # Cast Object to Subtype
            df[item] = df[item].astype("category")
            dataList_is_category.append(True)
        else:
            dataList_is_category.append(False)

is_CategorialColumn(df, column_names, dataList_is_category)

def isCategoricalColumnAPIFunction(df):
    
    return df.apply(lambda s: is_categorical(s))
#print(is_categorical(df.Assortment))
print(isCategoricalColumnAPIFunction(df).tolist())

"""
 Check for Dates
 Regex-Überprüfung auf Dates muss True sein als Kriterium, dass Software die Daten richtig erkannt hat.
 Unterscheidung alle Arten von Daten und Dates im Format Y-M-D.

 Annahme, die Methode to_datetime in Pandas kann Attribute, die ein Datum speichern in solche umwandeln. 
 Lediglich Attribute die bereits als Date erkannt wurden in die Liste aufnehmen.

"""


#Check whether an array-like or dtype is of the object dtype
#this solution does not detect datetime.date fields
def isDateColumnAPIFunction(df):
    
    return df.apply(lambda s: is_datetime(s))

def isDateColumn(df):
    return df.apply(lambda s: pd.to_datetime(s, format='%Y-%m-%d', errors='coerce').notnull().all())

def isDateColumnOnlyColumnsWithRegex(df, column_names):
    regexYMD = "^(19|20)\d\d[- /.](0[1-9]|1[012])[- /.](0[1-9]|[12][0-9]|3[01])$"
    regexMDY = "^(0[1-9]|1[012])[- /.](0[1-9]|[12][0-9]|3[01])[- /.](19|20)\d\d$"
    regexDMY = "^(0[1-9]|[12][0-9]|3[01])[- /.](0[1-9]|1[012])[- /.](19|20)\d\d$"
    Series = df.apply(lambda row: row.astype(str).str.contains(regexYMD).all())
    return Series

def evaluatePandasDateDetection(column_names, dataList_is_date_pandas, dataList_is_date_regex):
    mistakeListPandas = []
    for index, (first, second) in enumerate(zip(dataList_is_date_pandas, dataList_is_date_regex)):
        if first != second:
            mistakeListPandas.append(True)
        else:
            mistakeListPandas.append(False)
    return mistakeListPandas    


dataList_is_date_pandas = isDateColumn(df).tolist()
dataList_is_date_pandas_APIFunction = isDateColumnAPIFunction(df).tolist()
#print(dataList_is_date_pandas_APIFunction)
dataList_is_date_regex  = isDateColumnOnlyColumnsWithRegex(df, column_names)
dataList_is_mistake_pandas = evaluatePandasDateDetection(column_names, dataList_is_date_pandas, dataList_is_date_regex)

#detect columns with specific datatype. Result is Dataframe which includes matched columns

# matches none
result1 = df.select_dtypes(include=[np.datetime64])
result3 = df.select_dtypes(include=['datetime'])

# matches all
result = df.select_dtypes(include=[datetime.datetime])



# Check if is_numeric. Drop missing values


def isNumericColumn(df):
    # check for categorical data with number of values.
    for idx, item in enumerate(column_names):
        if df[item].isnull().values.any():
            new_df = df[df[item].notna()]
    if df[item].isnull().values.any():
        return new_df.apply(lambda s: pd.to_numeric(s,errors='coerce').notnull().all())
    else:
        return df.apply(lambda s: pd.to_numeric(s,errors='coerce').notnull().all())

dataList_is_numeric = isNumericColumn(df).tolist()



def getDatatype(df, dataListDatatype):
    for idx, item in enumerate(column_names):
        dataListDatatype.insert(idx,df.dtypes.loc[item].name)
    
getDatatype(df,dataListDatatype)

#Check relative Number must be between 0 and 1
def isRelativeNumber(df, column_names,dataList_is_relativeNumber):
    for idx, item in enumerate(column_names):
        in_between = df[item].between(0, 1,inclusive=True).all()
        if in_between == True:
            dataList_is_relativeNumber.append(True)
        else:
            dataList_is_relativeNumber.append(False)


def isRelativeNumber_onlyFloat(df, column_names,dataList_is_relativeNumber, dataListDatatype):
    for itemColumn_Names, itemDatatype in zip(column_names, dataListDatatype):
        if itemDatatype != 'float64':
            dataList_is_relativeNumber.append(False)
            continue
        else:            
            in_between = df[itemColumn_Names].between(0, 1,inclusive=True).all()
            if in_between == True:
                dataList_is_relativeNumber.append(True)
            else:
                dataList_is_relativeNumber.append(False)

isRelativeNumber_onlyFloat(df, column_names, dataList_is_relativeNumber, dataListDatatype)





dffinal = {column_names_metadata[0]: dataListDatatype,
        column_names_metadata[1]: dataList_is_numeric,
        column_names_metadata[2]:dataList_is_relativeNumber,
        column_names_metadata[3]: dataList_is_date_regex,
        column_names_metadata[4]:dataList_is_category,
        'attributs':column_names}
dffinal = pd.DataFrame(dffinal)


# Zeitreihenanalyse

DateColumnRow = dffinal[dffinal["is_date"]==True]
DateColumnNames = DateColumnRow.index.values.tolist()




# Ausreißer identifizieren
# Kriterium für ermittelte Ausreißer: Einer davon muss MIN-Wert sein bzw. MAX-Wert. 

dffinal.set_index('attributs', inplace=True)

dffinal.to_csv('Metadata_Rossmann.csv')
