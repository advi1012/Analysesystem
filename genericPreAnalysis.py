#Algorithmus

import numpy as np
import pandas as pd
import datetime
import json
from pandas.api.types import is_datetime64_any_dtype as is_datetime
from pandas.api.types import is_categorical_dtype as is_categorical
import regex
import re



class GenericPreAnalysis:

	def __init__(self, dataframe):
		self.dataframe = dataframe

	# result is the opposit of is_numeric
	# Todo: Heuristik. https://stackoverflow.com/questions/35826912/what-is-a-good-heuristic-to-detect-if-a-column-in-a-pandas-dataframe-is-categori
	def is_CategorialColumn(self, listOfCurrencys, column_names, dataList_is_category, dataList_is_date_regex, dataListCurrencyUnit):
		for itemColumn_Names, itemDateRegex, itemCurrency in zip(column_names, dataList_is_date_regex, dataListCurrencyUnit):
			#print(self.dataframe["Open"].dtype.name == "category") => All False		
			if self.dataframe[itemColumn_Names].dtype == pd.CategoricalDtype:
				if itemDateRegex == True:
					# Method pd.to_datetime returns datetime64[ns]. Required for resampling a TimeSeries
					self.dataframe[itemColumn_Names] = pd.to_datetime(self.dataframe[itemColumn_Names])
					#if itemColumn_Names == "Date" and self.dataframe[itemColumn_Names].dtype == "datetime64[ns]":
					#	print("HiDate")
					dataList_is_category.append(True)
					continue
				if itemCurrency in listOfCurrencys:

					
					#remove everything except the number
					#self.dataframe[itemColumn_Names] = self.dataframe[itemColumn_Names].str.replace(r'[^\d.,]+', '')
					self.dataframe[itemColumn_Names] = self.dataframe[itemColumn_Names].apply(self.trimString)
					self.dataframe[itemColumn_Names] = self.dataframe[itemColumn_Names].astype("int64")
					dataList_is_category.append(False)					
					continue
				#Set Categorical Column True
				dataList_is_category.append(True)
				# Cast Object to subtype		 
				self.dataframe[itemColumn_Names] = self.dataframe[itemColumn_Names].astype("category")
			else:
				dataList_is_category.append(False)

	def trimString(self, s):
		return re.compile(r'[^\d.,]+').sub('', s)

	def isCategoricalColumnAPIFunction(self,df):

		return df.apply(lambda s: is_categorical(s))



	def compareFunctionsCategorical(self, dataList_is_category, dataList_is_categoryAPIFunction):
		return ""

	#Check whether an array-like or dtype is of the object dtype
	#this solution does not detect datetime.date fields
	def isDateColumnAPIFunction(self):

		return self.dataframe.apply(lambda s: is_datetime(s))

	def isDateColumn(self, format='%Y-%m-%d'):
		return self.dataframe.apply(lambda s: pd.to_datetime(s, format=format, errors='coerce').notnull().all())

	def isDateColumnOnlyColumnsWithRegex(self, format = '%Y-%m-%d'):
		
		regex = "^(19|20)\d\d[- /.](0[1-9]|1[012])[- /.](0[1-9]|[12][0-9]|3[01])$"
		if format == '%m-%d-%Y':
			regex = "^(0[1-9]|1[012])[- /.](0[1-9]|[12][0-9]|3[01])[- /.](19|20)\d\d$"
		elif format == '%d-%m-%Y':
			regex = "^(0[1-9]|[12][0-9]|3[01])[- /.](0[1-9]|1[012])[- /.](19|20)\d\d$"
		Series = self.dataframe.apply(lambda row: row.astype(str).str.contains(regex).all())
		return Series
	
	def setDateFormat(self, dataList_is_date_regex, dataList_is_date_regex_YMD, dataList_is_date_regex_MDY, dataList_is_date_regex_DMY, dataListDateFormat):
		for YMD, MDY, DMY in zip(dataList_is_date_regex_YMD, dataList_is_date_regex_MDY, dataList_is_date_regex_DMY):
			if YMD == True:
				dataListDateFormat.append('YMD')
				dataList_is_date_regex.append(True)
			elif MDY == True:
				dataListDateFormat.append('MDY')
				dataList_is_date_regex.append(True)
			elif DMY == True:
				dataListDateFormat.append('DMY')
				dataList_is_date_regex.append(True)
			else:
				dataList_is_date_regex.append(False)
				dataListDateFormat.append('None')

	def evaluatePandasDateDetection(self, dataList_is_date_pandas, dataList_is_date_regex):
		mistakeListPandas = []
		for index, (first, second) in enumerate(zip(dataList_is_date_pandas, dataList_is_date_regex)):
			if first != second:
				mistakeListPandas.append(True)
			else:
				mistakeListPandas.append(False)
		return mistakeListPandas

	#detect columns with specific datatype. Result is Dataframe which includes matched columns
	# The subset of the frame including the dtypes in include and excluding the dtypes in exclude
	def selectDtypes(self):

		# matches none
		resultDatetime64 = self.dataframe.select_dtypes(include=[np.datetime64])
		resultDatetimeString= self.dataframe.select_dtypes(include=['datetime'])

		# matches all / Should not be used 
		resultDatetime = self.dataframe.select_dtypes(include=[datetime.datetime])


		return list(resultDatetime64), list(resultDatetimeString)



	# Check if is_numeric. Do not consider missing values


	def isNumericColumn(self, column_names):
		# if check for null values is successfull Sales will be Numeric
		for idx, item in enumerate(column_names):
			if self.dataframe[item].isnull().values.any():
				new_df = self.dataframe[self.dataframe[item].notna()]
		if self.dataframe[item].isnull().values.any():
			return new_df.apply(lambda s: pd.to_numeric(s,errors='coerce').notnull().all())
		else:
			# to_numeric can not convert columns from Datatype float64
			return self.dataframe.apply(lambda s: pd.to_numeric(s,errors='coerce').notnull().all())

	"""
	def isCoordinateCandidate(self, column_names):
		for idx, item in enumerate(column_names):
			if ',' in 
	"""


	def setDatatype(self,column_names, dataListDatatype):
		for idx, item in enumerate(column_names):
			dataListDatatype.insert(idx,self.dataframe.dtypes.loc[item].name)


	#Check relative Number must be between 0 and 1
	def isRelativeNumber(self, column_names,dataList_is_relativeNumber):
		for idx, item in enumerate(column_names):
			in_between = self.dataframe[item].between(0, 1,inclusive=True).all()
			if in_between == True:
				dataList_is_relativeNumber.append(True)
			else:
				dataList_is_relativeNumber.append(False)


	def isRelativeNumberOnlyFloat(self, column_names,dataList_is_relativeNumber, dataListDatatype):
		for itemColumn_Names, itemDatatype in zip(column_names, dataListDatatype):
			if itemDatatype != 'float64':
				dataList_is_relativeNumber.append(False)
				continue
			else:			
				in_between = self.dataframe[itemColumn_Names].between(0, 1,inclusive=True).all()
				if in_between == True:
					dataList_is_relativeNumber.append(True)
				else:
					dataList_is_relativeNumber.append(False)


	def setCurrencyUnit(self, column_names, dataListCurrencyUnit, curr="$€£"):
		resultListOfCurrencys = []
		for idx, item in enumerate(column_names):
			listOfCurrencys = regex.findall(r'\p{Sc}', str(self.dataframe[item].loc[~self.dataframe[item].isnull()].iloc[0]))
			if len(listOfCurrencys) == 1:
				dataListCurrencyUnit.append(listOfCurrencys[0])
				resultListOfCurrencys = listOfCurrencys
			elif not listOfCurrencys:
				dataListCurrencyUnit.append("None")
			else:
				dataListCurrencyUnit.append(listOfCurrencys)
				resultListOfCurrencys = listOfCurrencys
		return resultListOfCurrencys






#def getDataframeFinalMetadata():
#	return dffinal
