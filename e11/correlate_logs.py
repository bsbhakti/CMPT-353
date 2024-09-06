import sys
assert sys.version_info >= (3, 8) # make sure we have Python 3.8+
from pyspark.sql import SparkSession, functions, types, Row
from pyspark.sql.functions import col, sum as spark_sum, count
import re
import math


line_re = re.compile(r"^(\S+) - - \[\S+ [+-]\d+\] \"[A-Z]+ \S+ HTTP/\d\.\d\" \d+ (\d+)$")


def line_to_row(line):
    """
    Take a logfile line and return a Row object with hostname and bytes transferred.
    Return None if regex doesn't match.
    """
    m = line_re.match(line)
    if m:
        return Row(m[1],m[2])

    else:
        return None


def not_none(row):
    """
    Is this None? Hint: .filter() with it.
    """
    return row is not None


def create_row_rdd(in_directory):
    # TODO: return an RDD of Row() objects
    log_lines = spark.sparkContext.textFile(in_directory)
    log_lines = log_lines.map(line_to_row)
    # print(log_lines.take(5))
    return log_lines.filter(not_none)



def main(in_directory):
    logs = spark.createDataFrame(create_row_rdd(in_directory))
    logs = logs.groupBy("_1").agg(
    spark_sum("_2").alias("sum_2"),
    count("_2").alias("count_2"))
    logs = logs.withColumn("x_sq",logs["count_2"] ** 2).withColumn("y_sq",logs["sum_2"] **2).withColumn("xy",logs["count_2"] * logs["sum_2"])
    # logs.show()
    logs = logs.groupBy().agg(
        spark_sum("count_2").alias("x"), spark_sum("sum_2").alias("y"), spark_sum("x_sq"), spark_sum("y_sq"), spark_sum("xy"), count("sum_2").alias("n")
    )
    f = logs.first()
    num = (f["n"] *f["sum(xy)"]) - (f["x"] * f["y"])
    den = math.sqrt(
    (f["n"] * f["sum(x_sq)"] - f["x"]**2) * 
    (f["n"] * f["sum(y_sq)"] - f["y"]**2)
)

    r = num/den # TODO: it isn't zero.
    print(f"r = {r}\nr^2 = {r*r}")
    # Built-in function should get the same results.
    #print(totals.corr('count', 'bytes'))


if __name__=='__main__':
    in_directory = sys.argv[1]
    spark = SparkSession.builder.appName('correlate logs').getOrCreate()
    assert spark.version >= '3.2' # make sure we have Spark 3.2+
    spark.sparkContext.setLogLevel('WARN')

    main(in_directory)
