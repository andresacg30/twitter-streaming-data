from asyncio import sleep
import findspark
findspark.init()

from collections import namedtuple
from pyspark.sql.functions import desc
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession, SQLContext


def start_spark_session(host, port):
    spark_session = SparkSession.builder.appName('tweet_listener').getOrCreate()
    spark_context = spark_session.sparkContext
    streaming_context = StreamingContext(spark_context, 10)
    sql_context = SQLContext(sparkContext=spark_context, sparkSession=spark_session)

    socket_stream = streaming_context.socketTextStream(host, port)
    tweet_lines = socket_stream.window(60)

    fields = ("hashtag", "count")
    tweet = namedtuple('Tweet', fields)
    

    (tweet_lines.flatMap(
        lambda text: text.split(" ")
    ).filter(
        lambda word: word.lower().startswith("#")
    ).map(
        lambda word: (word.lower(), 1)
    ).reduceByKey(
        lambda a, b: a + b
    ).map(
        lambda rec: tweet(rec[0], rec[1])
    ).foreachRDD(
        lambda rdd: rdd.toDF().sort(
            desc("count")
            ).limit(10).registerTempTable("tweets")
    )
    )
    streaming_context.start()
