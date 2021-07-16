import numpy as np
import pandas as pd
#from run import dffinal
#from run import df

# Time Series Analysis

def preparetimeSeriesAnalysis(dffinalmetadata, df):

    dateColumnRow = dffinalmetadata[dffinalmetadata["is_date"]==True]
    dateColumnNames = dateColumnRow.index.values.tolist()

    # Allow only one dateColumn 

    if not dateColumnNames:
        print("No DateColumn detected. Can not perform time Series Analysis")
        return (None,None)
    else: 
        dateColumnName = dateColumnNames[0]
    # Detection is optimized for categorical and datetime attributs.
    # Apply Aggregation methods on every other column  Sum, Average (Assumption)
    # As wel as columns with currency Unit

        candidatsAggregationRow = dffinalmetadata[(dffinalmetadata["is_categoricalColumn"]==False) & (dffinalmetadata["is_date"] == False) | (dffinalmetadata["currencyUnit"] != "None")]
        candidatsAggregationNames = candidatsAggregationRow.index.values.tolist()

    # first analysis resamples date monthly
    # must set dateColumn as index. DateColumn must be of Type 
        #aggregation_spec_sum_avg = {s:[np.sum, np.avg] for s in candidatsAggregationNames} 
        aggregation_spec_sum_avg = {s:["sum",np.average] for s in candidatsAggregationNames} 
        #aggregation_spec_avg = {s:"avg" for s in candidatsAggregationNames}
        #aggregation_spec_sum_avg_c = dict(aggregation_spec_sum)
        #aggregation_spec_sum_avg_c.update(aggregation_spec_avg) 

        df[dateColumnName] = pd.to_datetime(df[dateColumnName])
        df.set_index(dateColumnName, inplace =True)

        return (aggregation_spec_sum_avg , candidatsAggregationNames)

def resampleByPeriodAll(aggregation_spec_sum_avg, candidatsAggregationNames, df, period="Q"):

    resampledDataFramePeriod = df.resample(period).agg(aggregation_spec_sum_avg)


# detect Outliers. IQR formula


    for item in candidatsAggregationNames:
        #Sum items
        q1Sum, q3Sum = resampledDataFramePeriod[item,'sum'].quantile([0.25, 0.75])

        iqr_AllStores_grp = q3Sum - q1Sum

        lower_bound = q1Sum - (1.5*iqr_AllStores_grp)

        upper_bound = q3Sum + (1.5*iqr_AllStores_grp)

        resampledDataFramePeriod[item,'sum_Ausreiser'] = ((resampledDataFramePeriod[item,'sum'] > upper_bound) | (resampledDataFramePeriod[item,'sum'] < lower_bound)).astype('int')

        #AVG items
        q1Avg, q3Avg = resampledDataFramePeriod[item,'average'].quantile([0.25, 0.75])

        iqr_AllStores_grp = q3Avg - q1Avg

        lower_bound = q1Avg - (1.5*iqr_AllStores_grp)

        upper_bound = q3Avg + (1.5*iqr_AllStores_grp)

        resampledDataFramePeriod[item,'average_Ausreiser'] = ((resampledDataFramePeriod[item,'average'] > upper_bound) | (resampledDataFramePeriod[item,'average'] < lower_bound)).astype('int')



    # resampledDataFramePeriod.to_csv('Zeitreihe_'+period+'.csv', encoding='utf-8-sig')
    # result = resampledDataFramePeriod.to_json('Zeitreihe_'+period+'.json',orient="index")
    # print(result)
    return resampledDataFramePeriod

def resampleByPeriodOnce(resampledDataFramePeriod, period="Q" ):
    # Sum or aggregate all from same period along the years. 2013-2014 All. 2015 till end of Juli. Check if whole year is covered.
    return ""
