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
    inputDirname = sys.argv[1]
    sparkSession = SparkSession.builder.appName(
        "meteo ho ho"
    ).master(
        "local[2]"
    ).getOrCreate()

    rdd = sparkSession.sparkContext.textFile(inputDirname).map(
        extract
    ).filter(
        removeInvalid
    ).map(
        prepareForDataFrame
    )

    schema = [
         "year",
         "month",
         "temperature",
         "quality"
    ]
    df = sparkSession.createDataFrame(rdd, schema)

    from pyspark.sql import functions as fns
    aggTuple = (
        fns.min(df.temperature),
        fns.max(df.temperature),
        fns.avg(df.temperature)
    )
    elemList = df.groupBy(
        df.month
    ).agg(
        *aggTuple
    ).orderBy(
        df.month
    ).collect()
    
    for elem in elemList:
        print (elem)
