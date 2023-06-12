from pyspark.sql.functions import udf, countDistinct, sum, col
from pyspark.sql.types import IntegerType
from pyspark.sql.dataframe import DataFrame

calculate_time_overlap = lambda start_interval_1, end_interval_1, start_interval_2, end_interval_2: \
    0 if end_interval_1 <= start_interval_2 or end_interval_2 <= start_interval_1 else \
    max(end_interval_1, end_interval_2) - min(start_interval_1, start_interval_2) 

calculate_time_overlap_udf = udf(calculate_time_overlap, IntegerType())

def compute_heatmap(cows_bff: DataFrame):
    cow1 = cows_bff\
        .withColumnRenamed('cow_name','cow1')\
        .withColumnRenamed('meal_start','meal_start1')\
        .withColumnRenamed('meal_end','meal_end1')\
        .withColumnRenamed('date','date1')\
        .select('cow1','meal_start1','meal_end1','date1')


    cow2 = cows_bff\
        .withColumnRenamed('cow_name','cow2')\
        .withColumnRenamed('meal_start','meal_start2')\
        .withColumnRenamed('meal_end','meal_end2')\
        .withColumnRenamed('date','date2')\
        .select('cow2','meal_start2','meal_end2','date2')

    df = cow1.crossJoin(cow2)\
    .where((cow1.cow1 != cow2.cow2) & (cow1.date1 == cow2.date2))

    df = df\
        .withColumn('overlap', calculate_time_overlap_udf("meal_start1", "meal_end1", "meal_start2", "meal_end2"))\
        .select('cow1','cow2','date1','overlap')

    df = df.groupBy('cow1', 'cow2').agg(sum('overlap').alias('total_overlap'), countDistinct('date1').alias('distinct_days'))
    df = df\
        .withColumn('avg_overlap', df.total_overlap / df.distinct_days)\
        .select('cow1','cow2','avg_overlap')\
        .withColumnRenamed('avg_overlap','distance')\
        .sort(col('cow1').asc(), col('cow2').asc())

    return df

# make it so that we can also run the module using DBConnect
if __name__ == "__main__":
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.getOrCreate()

    cows_bff = spark.read.table("db.cows_bff")
    df = compute_heatmap(cows_bff)
    df.show()
