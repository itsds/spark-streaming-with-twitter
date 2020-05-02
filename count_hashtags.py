import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, explode, split
from pyspark.sql.types import StringType

if __name__ == "__main__":
    if len(sys.argv)<3:
        print("Invalid input argument")
        exit(-1)

    host = sys.argv[1]
    port = int(sys.argv[2])

    spark = SparkSession.builder.appName("count_hastag").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    lines = spark.readStream\
                .format("socket")\
                .option("host", host)\
                .option("port", port)\
                .load()
    words = lines.select(explode(split(lines.value," ")).alias("word"))
    def my_udf(word):
        if str(word).startswith("#"):
            return word
        else:
            return "nontag"

    extract_udf = udf(my_udf,StringType())
    resultDF = words.withColumn("tag",extract_udf(words.word))
    hastagCounts = resultDF.where(resultDF.tag!='nontag').groupBy(resultDF.tag).count().orderBy("count",ascending=False)
    query = hastagCounts.writeStream\
                        .outputMode("complete")\
                        .format("console")\
                        .option("truncate","false")\
                        .start()\
                        .awaitTermination()

