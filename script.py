
'''
    Driver for updating the CSVs for visualization -
    prompt the user to enter the year they want to add/update data for
'''
from data_processing import *
from data_storage import *
import pyspark
from sqlalchemy import create_engine

#given that data (3 csv files per year) are saved in VAERS-data folder

def main():

    #ask which year to update/add data for
    year = int(input("Enter year to add/update data to database:\n"))

    #update/add data
    db_name = 'vaersdata.db'
    make_tables(db_name, year)


    #ask if want to create the visualizations
    option_vis = int(input("Enter 1 for update and 0 for no update to vis"))

    #update/make vis files if yes
    if option_vis == 1:
        #extract data from sqlite then roughly clean for covid data and save to csv
        engine = create_engine("sqlite:///vaersdata.db")

        #make sure three tables are in db first
        assert len(engine.table_names()) == 3, "Need 3 tables imported"

        q1 = '''
        SELECT v.vax_manu, v.vax_dose_series, d.v_adminby, d.vax_date, d.numdays, d.age_yrs, v.vaers_id
        FROM vax v
        INNER JOIN data d
        ON v.vaers_id = d.vaers_id
        WHERE v.vax_type = 'COVID19';
        '''
        df1 = pd.read_sql(q1, engine)
        summary_df = make_summary_df(df1)
        summary_df.to_csv('stats.csv')

        q2 = '''
        SELECT s.vaers_id, s.year, v.vax_manu, v.vax_dose_series,
        GROUP_CONCAT(COALESCE(s.symptom1, ''),', ') ||', '||
        GROUP_CONCAT(COALESCE(s.symptom2, ''),', ') ||', '||
        GROUP_CONCAT(COALESCE(s.symptom3, ''),', ') ||', '||
        GROUP_CONCAT(COALESCE(s.symptom4, ''),', ') ||', '||
        GROUP_CONCAT(COALESCE(s.symptom5, ''), ', ') AS all_symptoms
        FROM symptoms s
        INNER JOIN vax v ON
        s.vaers_id = v.vaers_id
        WHERE v.vax_type = 'COVID19'
        GROUP BY s.vaers_id;
        '''

        df2 = pd.read_sql(q2, engine)
        ade_df = make_ade_df(df2)
        ade_df.to_csv('ade.csv')

        #process csv files with Spark to get desired tables, then save as csv for vis
        spark = pyspark.sql.SparkSession.builder.config('spark.driver.memory', '6g').getOrCreate()

        stats_spark_df = spark.read.csv('stats.csv', sep=",", inferSchema="true", header="true")
        ades_spark_df = spark.read.csv('ade.csv',sep=",", inferSchema="true", header="true")

        data_vis_df = make_data_vis(stats_spark_df)
        data_vis_df.to_csv('stats_vis.csv')

        ades_vis_df = make_ade_vis(ades_spark_df)
        ades_vis_df.to_csv('top_ades.csv')


        print("Updated 2 vis files")

main()
