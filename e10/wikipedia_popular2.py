import sys
from pyspark.sql import SparkSession, functions, types
import os
import re

spark = SparkSession.builder.appName('reddit averages').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

assert sys.version_info >= (3, 8) # make sure we have Python 3.8+
assert spark.version >= '3.2' # make sure we have Spark 3.2+

pageCounts_schema = types.StructType([
    types.StructField('language', types.StringType()),
    types.StructField('title', types.StringType()),
    types.StructField('requested', types.LongType()),
    types.StructField('bytes', types.LongType())
])

# Used online resources to figure out the regex pattern and matching to get the needed string
# write a function that takes pathnames and returns string like we need: '20160801-12'
# The filenames will be in the format pagecounts-YYYYMMDD-HHMMSS*. Extract the YYYYMMDD-HH substring of that as a label for the day/hour.
def extract_label_from_filename(filepath):
    # Extract filename from the full path
    filename = os.path.basename(filepath)

    # Use regular expression to match the pattern 'pagecounts-YYYYMMDD-HHMMSS*'
    match = re.match(r'pagecounts-(\d{8}-\d{2}).*', filename)

    if match:
        # Extract the matched substring (YYYYMMDD-HH)
        label = match.group(1)
        return label
    else:
        # Return an error message or handle the case where the filename doesn't match the expected pattern
        return f"Error: Filename '{filename}' does not match the expected pattern."


def main(in_directory, out_directory):
    
    # Convert the Python function to a Spark UDF
    path_to_hour_udf = functions.udf(extract_label_from_filename, types.StringType())
    
    pageCounts = spark.read.csv(in_directory, sep=' ',schema=pageCounts_schema).withColumn('filename', path_to_hour_udf(functions.input_file_name()))
    filteredPage = pageCounts.filter(pageCounts['language'] == 'en').filter(pageCounts['title'] != 'Main_Page').filter(~pageCounts['title'].startswith('Special:')).withColumnRenamed('filename', 'ExcludedFilename')
    
    filteredPage = filteredPage.cache()
    
    filteredPage = filteredPage.groupBy('ExcludedFilename').max('requested')
    final = pageCounts.join(filteredPage, (filteredPage['ExcludedFilename'] == pageCounts['filename']) & (filteredPage['max(requested)'] == pageCounts['requested']))
    final = final.select(final['filename'], final['title'], final['requested'])
    final = final.orderBy('filename','title')
    final.write.csv(out_directory + '-pagecount', mode='overwrite')
    # final.show()

if __name__=='__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    main(in_directory, out_directory)
