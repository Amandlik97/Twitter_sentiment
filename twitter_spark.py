from pyspark.sql import SparkSession
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[*]").config("spark.mongodb.input.uri","mongodb://localhost:27017/NewDB.Dummy").config("spark.mongodb.output.uri","mongodb://locahost:27017/NewDB.Dummy").config("spark.jars.packages","org.mongodb.spark:mongo-spark-connector_2.12:3.0.0").getOrCreate()
df = spark.read.format("mongo").option("uri","mongodb://localhost:27017/NewDB.Dummy").load()
df.registerTempTable("Twitter")
#df.count()

##########Feature Selection############
#import langdetect import detect_

#Selecting required columns

sf=df.select(explode(split(df.text, "t_end")).alias("text"),"created_at",col("user.location").alias("Location"),"retweet_count",col("user.followers_count").alias("User_followers"),col("user.favourites_count").alias("favourites_count"),col("user.verified").alias("Verified User"),"lang")
sf=sf.select(F.regexp_replace('text', r'http\S+', '').alias("Text"),"created_at","Location","retweet_count","favourites_count","Verified User","User_followers","lang")
sf=sf.select(F.regexp_replace('Text', '@\w+', '').alias("text"),"created_at","Location","retweet_count","favourites_count","Verified User","User_followers","lang")
sf=sf.select(F.regexp_replace('text', '#', '').alias("text"),"created_at","Location","retweet_count","favourites_count","Verified User","User_followers","lang")
sf=sf.select(F.regexp_replace('text', 'RT', '').alias("text"),"created_at","Location","retweet_count","favourites_count","Verified User","User_followers","lang")
sf=sf.select(F.regexp_replace('text', ':', '').alias("Text"),from_unixtime(unix_timestamp('created_at', 'EEE MMM d HH:mm:ss z yyyy'),format="yyyy-MM-dd").alias('date'),"Location","User_followers","favourites_count","retweet_count","Verified User")
sf=sf.fillna({"Location": "unknown"})
sf=sf.fillna({"retweet_count": 0})
sf=sf.filter((col("lang") == 'en'))

#sf.show()
sf.select("Text").show(truncate=False)
#af.printSchema()
#af.show()
#sf.withColumn('newDate',f.date_sub("created_at",10)).show()
#df.show(2)

sf.where(col("retweet_count").isin({"0"})).count()

############Dumping data into Mongo###########
sf.write.format("mongo").option("uri","mongodb://localhost:27017/New_filtered.Data").mode("append").save()