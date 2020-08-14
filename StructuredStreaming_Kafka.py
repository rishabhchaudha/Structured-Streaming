from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

##### Jars Required #####
#spark-sql-kafka-0-10_2.11-2.4.6.jar
#kafka-clients-2.4.1.jar

spark = SparkSession.builder.appName("Structured_Streaming_Kafka_Integration").getOrCreate()
	

##### Read Stream Method to read the data from Kafka #####
df = spark \
    	.readStream \
    	.format("kafka") \
    	.option("kafka.bootstrap.servers", <<<Server Address>>>) \
    	.option("kafka.security.protocol", "SASL_PLAINTEXT") \
    	.option("kafka.sasl.kerberos.service.name", "kafka") \
    	.option("kafka.sasl.kerberos.principal", <<<Kerberos Principal>>>) \
	.option("kafka.sasl.mechanism", "GSSAPI") \
	.option("kafka.security.kerberos.keytab", <<<KeyTab File>>>) \
	.option("subscribe", <<<Kafka Topic Name>>>) \
	.option("startingOffsets","earliest" ) \
    	.load()

##### JSON Sample #####

#{
#   "id": "first",
#   "name": "sample",
#   "designation": "CEO"
#}


##### Defining JSON Schema #####
jsonschema = StructType([ StructField("id", StringType(), True), StructField("name", StringType(), True), StructField("designation", StringType(), True) ])

##### Process Method to Write the Data into Hive #####
def process(df,batch_id):
	df.write.mode("append").saveAsTable(<<<Hive Table Name>>>)
    

##### Starting the Stream #####
query1= df.selectExpr('CAST(value as String) as value').select(from_json('value',schema).alias('value')).select('value.*')\
.writeStream\
.foreachBatch(process)\
.start()\
.outputMode('append')\
.start()\
.awaitTermination()


spark.stop()
