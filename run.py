#Algorithmus

import numpy as np
import pandas as pd
import datetime
import json
import sys
from genericPreAnalysis import GenericPreAnalysis
from timeseriesAnalysis import preparetimeSeriesAnalysis
from timeseriesAnalysis import resampleByPeriodAll
from timeseriesAnalysis import resampleByPeriodOnce
from enrich import Enrich
from locale import atof, setlocale, LC_ALL
import time

start = time.time()
print("hello")


setlocale(LC_ALL, 'deu_deu')
#print(atof('123,456'))


#Read Data
inputTest = "Test"
inputBig = "input_Rossmann_data"
inputMiddle = "input_Rossmann_data_Evaluation_10000"
inputSmall = "input_Rossmann_data_Evaluation_1000"

#df = pd.read_csv('C:/Users/vincent.adam/Desktop/Thesis_Projects/Pandas/Analysemodul/input_Rossmann_data_Evaluation_10000.csv', sep=';')
#df = pd.read_csv('fz28_2021_04_edited.csv', sep=';')
df = pd.read_csv(inputBig+'.csv', sep=';')
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
dataListCoordinatesCandidat = []
dataListMissingValues = []
dataListIsState = []
column_names_metadata=['Datatype', 'is_numeric', 'is_relativeNumber','is_date', 'DateFormat','is_categoricalColumn', 'currencyUnit', 'coordinatesCandidat', 'missingValues', 'isState']


genericPreAnalysis = GenericPreAnalysis(df)

# Check Categorical data. Assumption: Dates (can be displayed as dimension in charts) aswell as alphanumerical data.
# In Pandas Report: State, PromoInterval, Promo/2, Assortment, StoreType, SchoolHoliday, Open, Date

listOfCurrencys = genericPreAnalysis.setCurrencyUnit(column_names, dataListCurrencyUnit)
dataList_is_date_regex_YMD = genericPreAnalysis.isDateColumnOnlyColumnsWithRegex(format='%Y-%m-%d')
dataList_is_date_regex_MDY = genericPreAnalysis.isDateColumnOnlyColumnsWithRegex(format='%m-%d-%Y')
dataList_is_date_regex_DMY = genericPreAnalysis.isDateColumnOnlyColumnsWithRegex(format='%d-%m-%Y')

genericPreAnalysis.setDateFormat(dataList_is_date_regex, dataList_is_date_regex_YMD, dataList_is_date_regex_MDY, dataList_is_date_regex_DMY, dataListDateFormat)



# Check Categorical data. Assumption: Dates (can be displayed as dimension in charts) aswell as alphanumerical data.
# In Pandas Report: State, PromoInterval, Promo/2, Assortment, StoreType, SchoolHoliday, Open, Date

genericPreAnalysis.is_CategorialColumn(listOfCurrencys, column_names, dataList_is_category, dataList_is_date_regex, dataListCurrencyUnit)



dataList_is_numeric = genericPreAnalysis.isNumericColumn(column_names)

# Check coordinates

genericPreAnalysis.isCoordinateCandidate(column_names, dataListCoordinatesCandidat)

# Run set Datatype after function call is_CategorialColumn
genericPreAnalysis.setDatatype(column_names, dataListDatatype)

#Check relative Number must be between 0 and 1
genericPreAnalysis.isRelativeNumberOnlyFloat(column_names, dataList_is_relativeNumber, dataListDatatype)

# Check missing Values

genericPreAnalysis.hasMissingValues(column_names, dataListMissingValues)

genericPreAnalysis.isState(column_names, dataListIsState)

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

dfevaluation.to_csv('C:/Users/vincent.adam/Desktop/Thesis_Projects/Pandas/Analysemodul/final/evaluation_Pandas_Rossmann.csv', encoding='utf-8-sig')

# Use dataList_is_date_regex for valid dates. 

dffinalmetadata = {column_names_metadata[0]: dataListDatatype,
        column_names_metadata[1]: dataList_is_numeric,
        column_names_metadata[2]:dataList_is_relativeNumber,
        column_names_metadata[3]: dataList_is_date_regex,
		column_names_metadata[4]: dataListDateFormat,
        column_names_metadata[5]:dataList_is_category,
        column_names_metadata[6]:dataListCurrencyUnit,
		column_names_metadata[7]:dataListCoordinatesCandidat,
        column_names_metadata[8]:dataListMissingValues,
        column_names_metadata[9]:dataListIsState,
        'attributs':column_names}
dffinalmetadata = pd.DataFrame(dffinalmetadata)
dffinalmetadata.set_index("attributs", inplace =True)

enricher = Enrich()
dffinalmetadata = enricher.enrich(dffinalmetadata)

output = inputBig.replace('input', 'output')
dffinalmetadata.to_csv('C:/Users/vincent.adam/Desktop/Thesis_Projects/Pandas/Analysemodul/final/'+output+'_Metadata_NumericCommaPoint'+'.csv', encoding='utf-8-sig')
dffinalmetadata.to_json('C:/Users/vincent.adam/Desktop/Thesis_Projects/Pandas/Analysemodul/final/'+output+'_Metadata_NumericCommaPoint'+'.json', orient="index")
end = time.time()
print(end - start)


# aggregation_spec_sum_avg, candidatsAggregationNames = preparetimeSeriesAnalysis(dffinalmetadata, df)


# if not aggregation_spec_sum_avg or not candidatsAggregationNames:
# 	sys.exit("No timeSeries analysis possible")
# else:
# 	resampledDataFramePeriodQ = resampleByPeriodAll(aggregation_spec_sum_avg, candidatsAggregationNames, df, period = 'Q')
# 	resampledDataFramePeriodM = resampleByPeriodAll(aggregation_spec_sum_avg, candidatsAggregationNames, df, period = 'M')

# 	if "Sales" in column_names:
# 		resampledDataFramePeriodQAggregated = resampleByPeriodOnce(resampledDataFramePeriodQ, period="Q")
# 		resampledDataFramePeriodQAggregated.to_csv('./final/timeseries/'+output+'SumSalesperiodQ.csv', encoding='utf-8-sig')
# 		resampledDataFramePeriodQAggregated.to_json('./final/timeseries/'+output+'SumSalesperiodQ.json', orient="index", date_format='iso')


# 	resampledDataFramePeriodQ.to_csv('./final/timeseries/'+output+'periodQ.csv', encoding='utf-8-sig')
# 	resampledDataFramePeriodM.to_csv('./final/timeseries/'+output+'periodM.csv', encoding='utf-8-sig')
# 	# Issue https://github.com/pandas-dev/pandas/issues/16492
# 	resampledDataFramePeriodQ.to_json('./final/timeseries/'+output+'periodQ.json', orient="index", date_format='iso')
# 	resampledDataFramePeriodM.to_json('./final/timeseries/'+output+'periodM.json', orient="index", date_format='iso')
