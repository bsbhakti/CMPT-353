import sys
from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('reddit averages').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

assert sys.version_info >= (3, 8) # make sure we have Python 3.8+
assert spark.version >= '3.2' # make sure we have Spark 3.2+


comments_schema = types.StructType([
    types.StructField('lang', types.StringType()),
    types.StructField('page', types.StringType()),
    types.StructField('count', types.IntegerType()),
    types.StructField('bytes', types.IntegerType())])

def getTime(filename):
    filename = filename.split("-")
    end = filename[-1].split('.')[0]
    date = filename[-2] + "-" +end
    return date

path_to_hour = functions.udf(getTime, returnType=types.StringType())

def main(in_directory, out_directory):
    wiki = spark.read.csv(in_directory, schema=comments_schema, sep=" ").withColumn('filename', functions.input_file_name())
    wiki = wiki.withColumn('time', path_to_hour(wiki['filename']))
    wiki = wiki.filter(
        (functions.col("lang") == 'en') &
          (functions.col("page") != 'Main_Page') & 
            (~functions.col("page").startswith('Special'))
    ).cache()
    wiki_g = wiki.groupBy("time").agg(functions.max("count")).cache()
    wiki_g.show()
    wiki = wiki.join(wiki_g.hint("broadcast"), on=((wiki['count'] == wiki_g['max(count)']) & (wiki['time'] == wiki_g['time']) ))
    wiki  = wiki.drop(wiki_g['time'],wiki_g['max(count)'],wiki['filename'],wiki['bytes'])

    wiki = wiki.sort(['time', 'page'])
    wiki.show()
    wiki.write.csv(out_directory, mode='overwrite')
    return



if __name__=='__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    main(in_directory, out_directory)