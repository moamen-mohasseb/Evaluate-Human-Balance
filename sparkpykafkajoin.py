from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

"""
Section1: Defining The The first Stream
The first stream will consume data from redis-server
"""
# create a StructType for the Kafka redis-server topic which has all changes made to Redis 

redis_server_schema = StructType([
    StructField("key", StringType(), True),
    StructField("existType", StringType(), True),
    StructField("Ch", StringType(), True),
    StructField("Incr", StringType(), True),
    StructField("zSetEntries", ArrayType(StructType([StructField("element", StringType(), True),
    StructField("score", StringType(), True)]), True), True)])


# create a StructType for the Customer JSON that comes from Redis

customer_schema = StructType([
    StructField("customerName", StringType(), True),
    StructField("email", StringType(), True ),
    StructField("phone", StringType(), True),
    StructField("birthDay", StringType(), True)
])

intermediate_element_schema = StructType([
    StructField("startTime",StringType(), True),
    StructField("stopTime",StringType(), True),
    StructField("testTime",StringType(), True),
    StructField("totalSteps",StringType(), True),
    StructField("customer",StringType(), True)
])

#create a spark application object

spark = SparkSession \
        .builder \
        .master("local[*]") \
        .config('spark.ui.port','3000') \
        .appName("Stedi") \
        .getOrCreate()
    

spark.sparkContext.setLogLevel('WARN')

# using the spark application object, read a streaming dataframe from the Kafka topic redis-server as the source
# Be sure to specify the option that reads all the events from the topic including those that were published before you started the spark stream

df = spark.readStream.format("kafka"). \
 option("kafka.bootstrap.servers", "localhost:9092"). \
 option("subscribe", "redis-server"). \
 option("startingOffsets", "earliest"). \
 option("maxOffsetPerTrigger", "200").load()

# cast the value column in the streaming dataframe as a STRING 

kafka_df = df.selectExpr("CAST (value as STRING)")

# parse the single column "value" with a json object in it, like this:

# storing them in a temporary view called RedisSortedSet

splitted_table = kafka_df\
        .select(from_json(col('value'), redis_server_schema).alias("Data"))\
        .select("Data.*")

splitted_table.createOrReplaceTempView('RedisSortedSet')

# TO-DO: execute a sql statement against a temporary view, which statement takes the element field from the 0th element in the array of structs and create a column called encodedCustomer
# the reason we do it this way is that the syntax available select against a view is different than a dataframe, and it makes it easy to select the nth element of an array in a sql column

encodedCustomer = spark.sql("select zsetentries[0]['element'] as encodedcustomer from RedisSortedSet")

# take the encodedCustomer column which is base64 encoded at first 
string_customer = encodedCustomer.select(from_json(unbase64(col('encodedcustomer')).cast("string"),intermediate_element_schema).alias("Data")).select("Data.*").select("customer")

Customer = string_customer.select(from_json(col("customer"),customer_schema).alias("Data")).select("Data.*")

# parse the JSON in the Customer record and store in a temporary view called CustomerRecords
Customer.createOrReplaceTempView("CustomerRecords")
                                  
# JSON parsing will set non-existent fields to null, so let's select just the fields we want, where they are not null as a new dataframe called emailAndBirthDayStreamingDF
                                  
emailAndBirthDayStreamingDF = spark.sql("select email, birthDay from CustomerRecords where email is not null and birthDay is not null")

# from the emailAndBirthDayStreamingDF dataframe select the email and the birth year (using the split function)
# Split the birth year as a separate field from the birthday
# Select only the birth year and email fields as a new streaming data frame called emailAndBirthYearStreamingDF

emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF.select(col('email'), split(col('birthDay'),'-')[0].alias('birthYear')).dropDuplicates() 

"""
Section2: Defining The second Stream
The second stream will consume data from stedi-events
"""

# using the spark application object, read a streaming dataframe from the Kafka topic stedi-events as the source
# Be sure to specify the option that reads all the events from the topic including those that were published before you started the spark stream
event_schema = StructType([
    StructField('customer',StringType(),True),
    StructField('score',StringType(),True),
    StructField('riskDate',StringType(),True)
])



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
 
customerAndRiskScoreStreamingDF = spark.sql("select customer, score from CustomerRisk").dropDuplicates()

"""
Section 3: Joining the two streams together to generate the last row
"""

emailAndBirthYearStreamingDF.createOrReplaceTempView('emailAndBirthYearStreamingDF')
customerAndRiskScoreStreamingDF.createOrReplaceTempView('customerAndRiskScoreStreamingDF')

# commenting the use of Spark SQL according to the first review
# final_df= spark.sql("""
#                     select b.customer, b.score, a.email, a.birthYear
#                     from emailAndBirthYearStreamingDF a
#                     inner join customerAndRiskScoreStreamingDF b
#                     on a.email = b.customer
#                     """).dropDuplicates()

final_df = emailAndBirthYearStreamingDF.join(customerAndRiskScoreStreamingDF, \
                                             emailAndBirthYearStreamingDF.email == customerAndRiskScoreStreamingDF.customer , \
                                             'inner').dropDuplicates()

query = final_df.selectExpr("to_json(struct(*)) AS value").writeStream \
        .outputMode('append') \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("FailOnDataLoss" , "false") \
        .option("checkpointLocation", "checkpoint") \
        .option("topic", "stedi_birth") \
        .start();

query_console = final_df.selectExpr("to_json(struct(*)) AS value").writeStream \
        .outputMode('append') \
        .format('console') \
        .option('truncate' , False) \
        .start() \




query.awaitTermination()
query_console.awaitTermination()

