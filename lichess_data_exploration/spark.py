from pyspark import SparkConf, SparkContext
from pyspark.sql.types import *
from pyspark.sql.types import Row
from pyspark.sql import SparkSession




def clean_rows(p):
    if len(p) >= 12:
        return p
    else:
        return [''] * 13


def initialize_spark_context():
    conf = (SparkConf()
            .setMaster("local")
            .setAppName("Lichess")
            .set("spark.executor.memory", "8g")
            .set("spark.executor.heartbeatInterval", "3000s")
            .set("spark.network.timeout", "3600s"))

    sc = SparkContext('local[2]', '', conf=conf)
    return sc


def is_relevant(x):
    relevant_fields = ['Event "', 'White "', 'Black "', 'Result "', 'UTCDate "', 'UTCTime "', 'WhiteElo "',
                       'BlackElo "', 'ECO "', 'Opening "', 'TimeControl "', 'Termination "']
    for field in relevant_fields:
        if field in x:
            return True
    return False


def save_filtered_rdd(sc, filename, output_file):
    rdd = sc.textFile(filename)
    rdd_filtered = rdd.filter(lambda x: is_relevant(x))
    rdd_with_index = rdd_filtered.zipWithIndex().map(lambda x: (x[1], x[0].replace('[', '').replace('"', ''))) \
        .map(lambda x: (x[0], ''.join(x[1].split(' ')[1:])))

    rdd_with_index.saveAsPickleFile(output_file)


def save_grouped_by_game_rdd(sc, filename, output_file):
    rdd = sc.pickleFile(filename)

    game_indexed_rdd = rdd.map(lambda x: (str(int(x[0]) // 12 + 1), x[1]))

    seqOp = (lambda x, y: x + y)
    combOp = (lambda x, y: x + y)

    temp = game_indexed_rdd.aggregateByKey('', seqOp, combOp)
    grouped_by_game_rdd = temp.map(lambda x: [x[0]] + x[1].split(']')[:12]).map(lambda p: clean_rows(p))
    grouped_by_game_rdd.saveAsPickleFile(output_file)

def save_spark_dataframe(sc, filename, output_file):
    rdd = sc.pickleFile(filename)

    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    games_data = rdd.map(lambda p: Row(Index=p[0], Event=p[1], White=p[2], Black=p[3], Result=p[4], UTCDate=p[5],
                                UTCTime=p[6], WhiteElo=p[7], BlackElo=p[8], ECO=p[9], Opening=p[10],
                                TimeControl=p[11], Termination=p[12]))

    schemaString = "Index Event White Black Result UTCDate UTCTime WhiteElo BlackElo ECO Opening TimeControl Termination"
    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
    schema = StructType(fields)
    df = spark.createDataFrame(games_data, schema)

    df.write.save(output_file)


if __name__ == '__main__':
    sc = initialize_spark_context()
    save_filtered_rdd(sc, 'lichess_db_standard_rated_2018-03.pgn', 'rdd')
    save_grouped_by_game_rdd(sc, 'rdd', 'aggregated')
    save_spark_dataframe(sc, 'aggregated', "chessgames.parquet")
