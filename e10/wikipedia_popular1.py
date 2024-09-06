import sys
import os
import re
from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('reddit averages').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

assert sys.version_info >= (3, 8) # make sure we have Python 3.8+
assert spark.version >= '3.2' # make sure we have Spark 3.2+


Counts_schema = types.StructType([
    types.StructField('language', types.StringType()),
    types.StructField('title', types.StringType()),
    types.StructField('requested', types.LongType()),
    types.StructField('bytes', types.LongType())
])

def convert_name(filename):
    name= os.path.basename(filename)
    match = re.match(r'pagecounts-(\d{8}-\d{2}).*', name)
    if match:
        return match.group(1)
    else:
        return None
    

def main(in_directory, out_directory):
    convert_filename = functions.udf(convert_name, returnType=types.StringType())
    counts = spark.read.csv(in_directory, schema=Counts_schema, sep=" ").withColumn('filename', convert_filename(functions.input_file_name()))
    # countsFiltered = counts.filter(functions.col('language') == 'en').filter(functions.col('title') != 'Main_Page:').filter(~functions.col('title').startswith('Special:')).withColumnRenamed('filename', 'ExcludedFilename')
    countsFiltered = counts.filter(counts['language'] == 'en').filter(counts['title'] != 'Main_Page').filter(~counts['title'].startswith('Special:')).withColumnRenamed('filename', 'NewFileName')


    countsFiltered = countsFiltered.cache()
    countsFiltered = countsFiltered.groupBy('NewFileName').max('requested')
    
    result= counts.join(countsFiltered, (counts['filename'] == countsFiltered['NewFileName']) & (counts['requested'] == countsFiltered['max(requested)']))
    #  .select(counts['filename'], counts['title'], counts['requested'])
    result= result.orderBy(functions.col('filename'),functions.col('title'))
    result.show()

    result.write.csv(out_directory, mode='overwrite')

if __name__=='__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    main(in_directory, out_directory)