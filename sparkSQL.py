import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession, Row

def extract(line):
    key = line[15:23]
    value = {
        'temperature': int(line[87:92])/10,
        'quality': int(line[92])
    }
    return (key, value)

def removeInvalid(elem):
    return (
        elem[1]['temperature'] <= 999.9
        and
        elem[1]['quality'] in [0,1,4,5,9]
    )

def prepareForDataFrame(elem):
    return  [
        elem[0][0:4], # YYYY
        elem[0][4:6], # MM
        elem[1]['temperature'],
        elem[1]['quality']
    ]

if __name__ == "__main__":
    sc = SparkContext(appName='Meteoho', master='local[2]')
    inputDirname = sys.argv[1]

    rdd = sc.textFile(inputDirname).map(
        extract
    ).filter(
        removeInvalid
    ).map(
        prepareForDataFrame
    )

    spark = SparkSession.builder.appName(
        "meteohohoho"
    ).config(
        "master", "local[2]"
    ).getOrCreate()

    schema = [
         #Row(name="year"),
         #Row(name="month"),
         #Row(name="temperature"),
         #Row(name="quality")
         "year",
         "month",
         "temperature",
         "quality"
    ]
    df = spark.createDataFrame(rdd, schema)

    from pyspark.sql import functions as fns
    elemList = df.groupBy(df.month).agg(
        fns.min(df.temperature),
        fns.max(df.temperature),
        fns.avg(df.temperature)
    ).orderBy(
        df.month
    ).collect()
    for elem in elemList:
        print (elem)
