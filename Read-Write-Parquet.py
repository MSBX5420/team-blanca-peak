%%configure -f
{"driverMemory": "6000M"}

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

rdd = sc.textFile("s3://team-blanca-peak/yellow_tripdata_2019-01.csv")
rdd

rdd.take(10)

df_new = rdd.\
    map(lambda x: x.split(",")).\
    filter(lambda x: len(x) == 18). \
    map(lambda x: {
        'VendorID':x[0],
        'tpep_pickup_datetime':x[1],
        'tpep_dropoff_datetime':x[2],
        'passenger_count':x[3],
        'trip_distance':x[4],
        'RatecodeID':x[5],
        'store_and_fwd_flag':x[6],
        'PULocationID':x[7],
        'DOLocationID':x[8],
        'payment_type':x[9],
        'fare_amount':x[10],
        'extra':x[11],
        'mta_tax':x[12],
        'tip_amount':x[13],
        'tolls_amount':x[14],
        'improvement_surcharge':x[15],
        'total_amount':x[16],
        'congestion_surcharge':x[17]})\
    .toDF()
df_new.show(5)


df_write = rdd.\
    map(lambda x: x.split(",")).toDF()
df_write.show(5)


df_write.write.parquet("s3://msbx5420-soumya/parquet")

read_df = sqlContext.read.parquet("s3://msbx5420-soumya/parquet")
read_df.show(5)

print(df_new.count())
print(read_df.count())

sc.stop()

import urllib
from io import StringIO

url = "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-01.csv"
response = urllib.urlopen(url)
#response = urllib.request.urlopen(url)
data = response.read()
text = data.decode('utf-8')
rdd = spark.read.csv(sc.parallelize(text.splitlines()))