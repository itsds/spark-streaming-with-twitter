from pyspark.sql import SparkSession
import sys

from pyspark.sql.functions import explode, udf, window, split
from pyspark.sql.types import StringType

if __name__ =="__main__":

    if len(sys.argv) < 3:
        print("Invalid input command")

    host = sys.argv[1]
    port = int(sys.argv[2])

    spark = SparkSession.builder\
                        .appName("count hastag using windows functions")\
                        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    lines = spark.readStream\
                .format("socket")\
                .option("host",host)\
                .option("port",port)\
                .option("includeTimestamp","true")\
                .load()

    words = lines.select(explode(split(lines.value," ")).alias("word"),
                         lines.timestamp)

    def extract_tag(word):
        if str(word).startswith('#'):
            return word
        else:
            return 'nontag'

    extract_tag_udf = udf(extract_tag,StringType())
    resultDF = words.withColumn('tag_column',extract_tag_udf(words.word))
    finalDF = resultDF.where(resultDF.tag_column!='nontag')\
                    .groupBy(window(resultDF.timestamp,"50 seconds","30 seconds"),resultDF.tag_column)\
                    .count()\
                    .orderBy("count",ascending=False)

    query = finalDF.writeStream\
                .outputMode("complete")\
                .format("console")\
                .option("truncate","false")\
                .start()\
                .awaitTermination()