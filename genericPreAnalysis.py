#Algorithmus

import numpy as np
import pandas as pd
import datetime
from pandas.api.types import is_datetime64_any_dtype as is_datetime
from pandas.api.types import is_categorical_dtype as is_categorical
import regex


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
 Clientseitig: Wahl der Charts. Zuweisung der Information welches Attribut an welche Achse

"""
dataListDatatype = []
dataList_is_numeric = []
dataList_is_relativeNumber = []
dataList_is_date_regex = []
dataList_is_date_pandas = []
dataList_is_category = []
dataListCurrencyUnit = []
column_names_metadata=['Datatype', 'is_numeric', 'is_relativeNumber','is_date', 'is_categoricalColumn', 'currencyUnit']

print(df["Date"].dtype == datetime.date)

# Check Categorical data. Assumption: Dates (can be displayed as dimension in charts) aswell as alphanumerical data.
# In Pandas Report: State, PromoInterval, Promo/2, Assortment, StoreType, SchoolHoliday, Open, Date


# result is the opposit of is_numeric
def is_CategorialColumn(df, column_names, dataList_is_category):
    for idx, item in enumerate(column_names):
        #print(df["Open"].dtype.name == "category") => All False        
        if(df[item].dtype == pd.CategoricalDtype):
            dataList_is_category.append(True)
            # Cast Object to subtype         
            df[item] = df[item].astype("category")
            
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



# Check if is_numeric. Do not consider missing values


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


def isRelativeNumberOnlyFloat(df, column_names,dataList_is_relativeNumber, dataListDatatype):
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

isRelativeNumberOnlyFloat(df, column_names, dataList_is_relativeNumber, dataListDatatype)

def getCurrencyUnit(df, column_names, dataListCurrencyUnit, curr="$€£"):
    for idx, item in enumerate(column_names):
        listOfCurrencys = regex.findall(r'\p{Sc}', str(df[item].loc[~df[item].isnull()].iloc[0]))
        print(listOfCurrencys)
        if len(listOfCurrencys) == 1:
            dataListCurrencyUnit.append(listOfCurrencys[0])
        elif not listOfCurrencys:
            dataListCurrencyUnit.append("None")
        else:
            dataListCurrencyUnit.append(listOfCurrencys)


getCurrencyUnit(df, column_names, dataListCurrencyUnit)

# Use dataList_is_date_regex for valid dates. 

dffinal = {column_names_metadata[0]: dataListDatatype,
        column_names_metadata[1]: dataList_is_numeric,
        column_names_metadata[2]:dataList_is_relativeNumber,
        column_names_metadata[3]: dataList_is_date_regex,
        column_names_metadata[4]:dataList_is_category,
        column_names_metadata[5]:dataListCurrencyUnit,
        'attributs':column_names}
dffinal = pd.DataFrame(dffinal)


# Time Series Analysis

dateColumnRow = dffinal[dffinal["is_date"]==True]
dateColumnNames = dateColumnRow.index.values.tolist()

# Allow only one dateColumn 

if not dateColumnNames:
    print("No DateColumn detected. Can not perform time Series Analysis")
else: 
    dateColumnName = dateColumnNames[0]
# Detection is optimized for categorical and datetime attributs.
# Apply Aggregation methods on every other column  Sum, Average (Assumption)
# As wel as columns with currency Unit

    candidatsAggregationRow = dffinal[(dffinal["is_categoricalColumn"]==False) & (dffinal["is_date"] == False) | (dffinal["currencyUnit"] != "None")]
    candidatsAggregationNames = candidatsAggregationRow.index.values.tolist()

    dfAggregation = df[candidatsAggregationNames]

# first analysis resamples date monthly
# must set dateColumn as index. DateColumn must be of Type 
    aggregation_spec_sum_avg = {s:[np.sum, np.avg] for s in candidatsAggregationNames} 
    aggregation_spec_sum = {s+"SUM":"sum" for s in candidatsAggregationNames} 
    aggregation_spec_avg = {s+"AVG":"avg" for s in candidatsAggregationNames}  

    df[dateColumnName] = pd.to_datetime(df[dateColumnName])
    df.set_index(dateColumnName, inplace =True)

    resampledDataFrameMonthly = df.resample('M').agg(aggregation_spec_sum_avg)

# detect Outliers. IQR formula
    

    for item in candidatsAggregationNames:

        q1Sum, q3Sum = resampledDataFrameMonthly[item+"_SUM"].quantile([0.25, 0.75])

        iqr_AllStores_grp = q3Sum - q1Sum

        lower_bound = q1Sum - (1.5*iqr_AllStores_grp)

        upper_bound = q3Sum + (1.5*iqr_AllStores_grp)

        resampledDataFrameMonthly[item+"_SUM_Ausreiser"] = ((resampledDataFrameMonthly[item] > upper_bound) | (resampledDataFrameMonthly[item] < lower_bound)).astype('int')

        q1Avg, q3Avg = resampledDataFrameMonthly[item+"_AVG"].quantile([0.25, 0.75])

        iqr_AllStores_grp = q3Avg - q1Avg

        lower_bound = q1Avg - (1.5*iqr_AllStores_grp)

        upper_bound = q3Avg + (1.5*iqr_AllStores_grp)

        resampledDataFrameMonthly[item+"_AVG_Ausreiser"] = ((resampledDataFrameMonthly[item] > upper_bound) | (resampledDataFrameMonthly[item] < lower_bound)).astype('int')



# second analysis resamples date per quarter

    resampledDataFrameQuarters = df.resample('Q').agg(aggregation_spec_sum_avg)



# detect Outliers

dffinal.set_index('attributs', inplace=True)
dffinal.to_csv('Metadata_Rossmann.csv', encoding='utf-8-sig')
