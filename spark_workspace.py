import pyspark
from pyspark.sql import SparkSession
import time

def dun(x):
    print('as:',x)
    return x+' 123456789'
spark = SparkSession.builder.master("local[1]") \
                    .appName('meenakshisundaram') \
                    .getOrCreate()
spark.conf.set("spark.sql.files.maxPartitionBytes", '800')

df=spark.read.text("/Users/monasekar/PycharmProjects/pythonProject/test.txt")
df.createOrReplaceTempView("table1")
spark.sql("select concat(value,' 123345677') as value from table1 limit 10").createOrReplaceTempView("table1")
spark.sql("select * from table1").show(1000,False)
spark.sql("select SPLIT(value,' ') as value from table1").createOrReplaceTempView("table1")
spark.sql("select * from table1").show(1000,False)
df_final=spark.sql("select explode(value) from table1")
time.sleep(2400)
df_final.write.parquet('output')


# rdd = spark.sparkContext.textFile("/Users/monasekar/PycharmProjects/pythonProject/test.txt")
# print(rdd.count())
# rdd5=rdd.flatMap(lambda x : {dun(x)})
# for rec in rdd5.collect():
#      print('rdd5:',rec)
#
# time.sleep(120)
# rdd2 = rdd5.flatMap(lambda x: x.split(" "))
#
# for rec in rdd2.collect():
#      print('rdd2:',rec)
#
# print(rdd2.count())
#time.sleep(2400)
#
# for rec in rdd5.collect():
#     print('rdd2:',rec)
#rdd2.show(1000)


# #print(rdd2.collect())
# rdd3 = rdd2.map(lambda x: (x,1))
# print(rdd3.count())
# #print(rdd3.collect())
# rdd4 = rdd3.reduceByKey(lambda a,b: a+b)
# #print(rdd4.collect())
# rdd5 = rdd4.map(lambda x: (x[1],x[0])).sortByKey()
# #Print rdd5 result to console
# #print(rdd5.collect())
# print(rdd5.first())
# print('Count:', rdd5.count())
# print('Count:'+str(rdd.count()))
