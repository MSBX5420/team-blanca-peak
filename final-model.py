
import pyspark.sql.functions as sf
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.tuning import CrossValidator
from pyspark.ml.tuning import ParamGridBuilder
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

sqlContext = SQLContext(sc)
rdd = sc.textFile("s3://msbx5420-2020/team-blanca-peak/yellow_tripdata_2019-01.csv")
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

# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.enabled", "true")

# Convert the Spark DataFrame back to a pandas DataFrame using Arrow
result_pdf = df_new.select("*").toPandas()
result_pdf.head()

#Register the people df as a table
df_new.registerTempTable("taxi")
#Perform SQL Query
taxi = spark.sql("SELECT * FROM taxi")
taxi.show(5)

taxi = taxi.na.fill(0)
taxi.dtypes
taxi.count()
taxi = spark.sql("SELECT DOLocationID,PULocationID,VendorID,extra,fare_amount,\
                 passenger_count,payment_type,tolls_amount,\
                 total_amount,trip_distance,tip_amount,'tip' FROM taxi")
taxi.show(5)

taxi = taxi.withColumn('tip', sf.when(sf.col('tip_amount') > 0, 1).otherwise(0))
taxi = taxi.drop('tip_amount')
taxi.show(5)
taxi.columns

categorical_columns = taxi.columns[:-1]
categorical_columns
stringindexer_stages = [StringIndexer(inputCol=c, outputCol='stringindexed_' + c) for c in categorical_columns]
# encode label column and add it to stringindexer stages
stringindexer_stages += [StringIndexer(inputCol='tip', outputCol='label')]

onehotencoder_stages = [OneHotEncoder(inputCol='stringindexed_' + c, outputCol='onehot_'+c) for c in categorical_columns]

feature_columns = ['onehot_' + c for c in categorical_columns]
vectorassembler_stage = VectorAssembler(inputCols=feature_columns, outputCol='features')

all_stages = stringindexer_stages + onehotencoder_stages + [vectorassembler_stage]
pipeline = Pipeline(stages=all_stages)

pipeline_model = pipeline.fit(taxi)

final_columns = feature_columns + ['features', 'label']
taxi_df = pipeline_model.transform(taxi).select(final_columns)
#taxi_df.show(5)
train, test = taxi_df.randomSplit([0.8, 0.2], seed=1234)

random_forest = RandomForestClassifier(featuresCol='features', labelCol='label')

param_grid = ParamGridBuilder().\
    addGrid(random_forest.maxDepth, [2, 3, 4]).\
    addGrid(random_forest.minInfoGain, [0.0, 0.1, 0.2, 0.3]).\
    build()
evaluator = BinaryClassificationEvaluator()
crossvalidation = CrossValidator(estimator=random_forest, estimatorParamMaps=param_grid, evaluator=evaluator)
crossvalidation_mod = crossvalidation.fit(taxi_df)

pred_test = crossvalidation_mod.transform(test)
pred_test.show(5)
label_pred_test = pred_test.select('label', 'prediction')
label_pred_test.rdd.zipWithIndex().countByKey()

print('Accuracy : ', evaluator.setMetricName('areaUnderROC').evaluate(pred_test))
print('Precision : ', evaluator.setMetricName('areaUnderPR').evaluate(pred_test))
#print('Precision : ', evaluator.setMetricName('precision').evaluate(pred_test))

