

from pyspark.sql import SparkSession
import time

def main():
    start_time = time.time()
    # create sparkcontext
    spark = SparkSession.builder.getOrCreate()

    sc = spark.sparkContext

    # read input file from hdfs
    input_data_path = 's3://msbx5420-2020/team-blanca-peak/yellow_tripdata_2019-01.csv'
    text_rdd = sc.textFile(input_data_path)

    # counts rdd
    df = text_rdd. \
        map(lambda x: x.split(",")). \
        filter(lambda x: len(x) == 18). \
        map(lambda x: {
        'VendorID': x[0],
        'tpep_pickup_datetime': x[1],
        'tpep_dropoff_datetime': x[2],
        'passenger_count': x[3],
        'trip_distance': x[4],
        'RatecodeID': x[5],
        'store_and_fwd_flag': x[6],
        'PULocationID': x[7],
        'DOLocationID': x[8],
        'payment_type': x[9],
        'fare_amount': x[10],
        'extra': x[11],
        'mta_tax': x[12],
        'tip_amount': x[13],
        'tolls_amount': x[14],
        'improvement_surcharge': x[15],
        'total_amount': x[16],
        'congestion_surcharge': x[17]}) \
        .toDF()
    df.show(5)

    # write counts dataframe to hdfs
    df.write.parquet('s3://msbx5420-2020/team-blanca-peak/data.parquet')

    exe_time = "seconds ---" + str(time.time() - start_time)
    res_rdd = sc.parallelize(exe_time).collect()
    #res_rdd.saveAsTextFile("s3://msbx5420-2020/team-blanca-peak/output.txt")
    df_outout = spark.createDataFrame(
        [
            (exe_time),
        ],
        ['Execution Time']
    )
    #df_outout.coalesce(1).write.format("text").option("header", "true").mode("append").save("s3://msbx5420-2020/team-blanca-peak/data.output.parquet")
    print(exe_time)
    # stop sparkcontext
    sc.stop()


if __name__ == "__main__":
    main()
