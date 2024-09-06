from bz2 import compress
import pandas as pd
import os
import numpy as np
import math
import sys
from pyspark.sql import SparkSession, functions, types, Row
from pyspark.sql.functions import split, explode, lower, col
import string, re
wordbreak = r'[%s\s]+' % (re.escape(string.punctuation),)  # regex that matches spaces and/or punctuation

# Initialize Spark session
spark = SparkSession.builder.appName('Map weather to Fire').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

spark = SparkSession.builder.appName('Map weather to Fire').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

assert sys.version_info >= (3, 8) # make sure we have Python 3.8+
assert spark.version >= '3.2' # make sure we have Spark 3.2+


def main():

    filename = sys.argv[1]
    output_dir = sys.argv[2]
    data = spark.read.text(filename)
    data.show()
    data = data.select(split(data.value,wordbreak,0).alias('extracted'))
    data = data.select(explode(data.extracted).alias('exploded'))
    data = data.select(lower(data["exploded"]).alias('word'))
    data = data.groupBy('word').count().orderBy('count',ascending=False)
    data = data.filter(data.word != "")


    # data.show()
    data.write.csv(output_dir,compression="None",mode='overwrite')

main()
