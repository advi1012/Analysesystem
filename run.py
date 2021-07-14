#Algorithmus

import numpy as np
import pandas as pd
import datetime
import json
from genericPreAnalysis import GenericPreAnalysis

#Read Data
df = pd.read_csv('store_data_erweitert_Euro.csv', sep=';')
#df = pd.read_csv('fz28_2021_04_edited.csv', sep=';')
column_names = df.columns.tolist()



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
dataListDateFormat = []
dataList_is_category = []
dataListCurrencyUnit = []
column_names_metadata=['Datatype', 'is_numeric', 'is_relativeNumber','is_date', 'DateFormat','is_categoricalColumn', 'currencyUnit']


genericPreAnalysis = GenericPreAnalysis(df)

# Check Categorical data. Assumption: Dates (can be displayed as dimension in charts) aswell as alphanumerical data.
# In Pandas Report: State, PromoInterval, Promo/2, Assortment, StoreType, SchoolHoliday, Open, Date

genericPreAnalysis.setCurrencyUnit(column_names, dataListCurrencyUnit)
dataList_is_date_regex_YMD = genericPreAnalysis.isDateColumnOnlyColumnsWithRegex(format='%Y-%m-%d')
dataList_is_date_regex_MDY = genericPreAnalysis.isDateColumnOnlyColumnsWithRegex(format='%m-%d-%Y')
dataList_is_date_regex_DMY = genericPreAnalysis.isDateColumnOnlyColumnsWithRegex(format='%d-%m-%Y')

genericPreAnalysis.setDateFormat(dataList_is_date_regex, dataList_is_date_regex_YMD, dataList_is_date_regex_MDY, dataList_is_date_regex_DMY, dataListDateFormat)

dataList_is_numeric = genericPreAnalysis.isNumericColumn(column_names)

genericPreAnalysis.setDatatype(column_names, dataListDatatype)

#Check relative Number must be between 0 and 1
genericPreAnalysis.isRelativeNumberOnlyFloat(column_names, dataList_is_relativeNumber, dataListDatatype)

# Check Categorical data. Assumption: Dates (can be displayed as dimension in charts) aswell as alphanumerical data.
# In Pandas Report: State, PromoInterval, Promo/2, Assortment, StoreType, SchoolHoliday, Open, Date

genericPreAnalysis.is_CategorialColumn(column_names, dataList_is_category, dataList_is_date_regex, dataListCurrencyUnit)

# for evaluation of Pandas Software
dataList_is_date_pandas_YMD = genericPreAnalysis.isDateColumn(format='%Y-%m-%d').tolist()
dataList_is_date_pandas_MDY = genericPreAnalysis.isDateColumn(format='%m-%d-%Y').tolist()
dataList_is_date_pandas_DMY = genericPreAnalysis.isDateColumn(format='%d-%m-%Y').tolist()

dataList_is_date_pandas_APIFunction = genericPreAnalysis.isDateColumnAPIFunction().tolist()

dataList_is_mistake_pandas_to_datetime_YMD = genericPreAnalysis.evaluatePandasDateDetection(dataList_is_date_pandas_YMD,dataList_is_date_regex)
dataList_is_mistake_pandas_to_datetime_MDY = genericPreAnalysis.evaluatePandasDateDetection(dataList_is_date_pandas_MDY,dataList_is_date_regex)
dataList_is_mistake_pandas_to_datetime_DMY = genericPreAnalysis.evaluatePandasDateDetection(dataList_is_date_pandas_DMY,dataList_is_date_regex)

dfevaluation = {"is_mistake_YMD": dataList_is_mistake_pandas_to_datetime_YMD,
				"is_mistake_MDY": dataList_is_mistake_pandas_to_datetime_MDY,
				"is_mistake_DMY": dataList_is_mistake_pandas_to_datetime_DMY,
				'attributs': column_names}
dfevaluation = pd.DataFrame(dfevaluation)
dfevaluation.set_index("attributs", inplace =True)

# Use dataList_is_date_regex for valid dates. 

dffinal = {column_names_metadata[0]: dataListDatatype,
        column_names_metadata[1]: dataList_is_numeric,
        column_names_metadata[2]:dataList_is_relativeNumber,
        column_names_metadata[3]: dataList_is_date_regex,
		column_names_metadata[4]: dataListDateFormat,
        column_names_metadata[5]:dataList_is_category,
        column_names_metadata[6]:dataListCurrencyUnit,
        'attributs':column_names}
dffinal = pd.DataFrame(dffinal)

dffinal.to_csv('Metadata_Rossmann.csv', encoding='utf-8-sig')
metadatajson = dffinal.to_json('Metadata_Rossmann.json', orient="index")
parsed  = json.loads(metadatajson)
print(json.dumps(parsed, indent=4))