from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

# using the spark application object, read a streaming dataframe from the Kafka topic stedi-events as the source
# Be sure to specify the option that reads all the events from the topic including those that were published before you started the spark stream
event_schema = StructType([
    StructField('customer',StringType(),True),
    StructField('score',StringType(),True),
    StructField('riskDate',StringType(),True)
])


spark = SparkSession \
        .builder \
        .master("local[*]") \
        .config('spark.ui.port','3000') \
        .appName("stedi-events") \
        .getOrCreate()

spark.sparkContext.setLogLevel('WARN')


df = spark.readStream.format("kafka"). \
 option("kafka.bootstrap.servers", "localhost:9092"). \
 option("subscribe", "stedi-events"). \
 option("startingOffsets", "earliest"). \
 option("maxOffsetPerTrigger", "200").load()

# cast the value column in the streaming dataframe as a STRING 
kafka_df = df.selectExpr("CAST (value as STRING)")

# parse the JSON from the single column "value" with a json object in it, like this:
# and create separated fields like this:

# storing them in a temporary view called CustomerRisk
customer = kafka_df\
        .select(from_json(col('value'), event_schema).alias("Data"))\
        .select("Data.*")

customer.createOrReplaceTempView('CustomerRisk')

#execute a sql statement against a temporary view, selecting the customer and the score from the temporary view, creating a dataframe called customerRiskStreamingDF
# sink the customerRiskStreamingDF dataframe to the console in append mode
 

customerRiskStreamingDF = spark.sql("select customer, score from CustomerRisk")

query = customerRiskStreamingDF.writeStream \
        .outputMode('append') \
        .format('console') \
        .option('truncate' , False) \
        .start() \

query.awaitTermination()

