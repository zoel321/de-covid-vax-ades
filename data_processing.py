from pyspark.sql.functions import mean, stddev, count
from pyspark.sql.functions import split, explode, lower
from pyspark.sql.functions import collect_list, concat_ws
from pyspark.sql.functions import udf
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
import re

#helper function
def group_multdoses(dose):
    if dose not in ['1','2','3', '4']:
        dose = '5+'
    return dose

def make_summary_df(df):
    df = df.dropna(subset = ['vax_dose_series', 'v_adminby', 'numdays', 'age_yrs', 'vax_date'])
    dict_location = {'PVT': 'Private', 'PHM': 'Pharmacy', 'MIL': 'Military', 'OTH': 'Other', 'PUB': 'Public', 'WRK': 'Workplace Clinic', 'UNK': 'Unknown', 'OTH': 'Other', 'SEN': 'Nursing Home', 'SCH': 'School'}
    df.replace({'v_adminby': dict_location}, inplace=True)
    df.vax_dose_series = df.vax_dose_series.apply(lambda x: group_multdoses(x))
    return df

def make_ade_df(df):
# getting more detailed df with symptoms, year, vaccine, and dose number
    df = df.dropna(subset = ['vax_manu', 'vax_dose_series'])
    df.vax_dose_series = df.vax_dose_series.apply(lambda x: group_multdoses(x))
    return df


#Spark functions
def make_data_vis(df):
    stats_vis = df.groupby(['vax_manu', 'vax_dose_series', 'v_adminby'])\
    .agg(mean('numdays').alias('avg_onset'),\
        stddev('numdays').alias('std_onset'),\
        mean('age_yrs').alias('avg_age'),\
        stddev('age_yrs').alias('std_age'),\
        count('vaers_id').alias('cases'))
    stats_vis_df = stats_vis.toPandas()
    return stats_vis_df

def make_ade_vis(df):
    #getting counts for symptoms
    df = df.select(['vax_manu', 'vax_dose_series', 'all_symptoms']).groupby(['vax_manu', 'vax_dose_series']).agg(concat_ws(',', collect_list(df.all_symptoms)).alias('symptoms'))
    comma_clean = udf(lambda x: re.sub(" ,","", x))
    df = df.withColumn('cleaned_symptoms', comma_clean('symptoms'))
    df = df.withColumn('cleaned_symptoms', explode(split(lower(col('cleaned_symptoms')), ','))).groupby(['vax_manu', 'vax_dose_series','cleaned_symptoms']).agg(count('cleaned_symptoms').alias('symptom_count'))

    #getting top 10 symptoms for each group
    column_list = ["vax_manu","vax_dose_series"]
    windowDept = Window.partitionBy([col(x) for x in column_list]).orderBy(col('symptom_count').desc())
    df = df.withColumn("row", row_number().over(windowDept)).filter(col("row") <= 10)
    top_ades_df = df.toPandas()
    return top_ades_df
