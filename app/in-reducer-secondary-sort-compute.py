import time
import re
from pyspark import SparkContext, SparkConf
from datetime import datetime


APP_NAME = 'in-reduce-secondary-sort-compute'
INPUT_FILE = '/data/Taxi_Trips.csv.xsmall'
OUTPUT_DIR = '/data/output-in-reduce-sort-compute-{timestamp}.txt'

COMMA_DELIMITER = re.compile(''',(?=(?:[^"]*"[^"]*")*[^"]*$)''')

FIRST_KEY = 1
SECOND_KEY = 2
TRIP_END_TIMESTAMP = 3

TIMESTAMP = int(time.time())


def make_pair(entry):
    key = entry[FIRST_KEY]
    return key, entry


def create_pair_rdd(ctx):
    rawRDD = ctx.textFile(INPUT_FILE)
    headerlessRDD = rawRDD.filter(lambda x: not x.startswith('Trip ID'))
    rdd = headerlessRDD.map(lambda x: COMMA_DELIMITER.split(x))
    validRDD = rdd.filter(lambda x: len(x[FIRST_KEY]) > 0 and len(x[SECOND_KEY]) > 0 and len(x[TRIP_END_TIMESTAMP]) > 0)
    pairRDD = validRDD.map(make_pair)
    compressedRDD = pairRDD.mapValues(lambda x: (x[SECOND_KEY], x[TRIP_END_TIMESTAMP]))

    return compressedRDD


def sort_group(group):
    return sorted(group, key=lambda entry: entry[0], reverse=False)


def calculate_loss(entry):
    group = entry
    loss = 0
    _, prev_end = group[0]

    for item in group:
        start, end = item
        delta = datetime.strptime(start, '%m/%d/%Y %I:%M:%S %p').timestamp()\
                - datetime.strptime(prev_end, '%m/%d/%Y %I:%M:%S %p').timestamp()
        if delta > 0:
            loss += delta
        prev_end = end

    return loss


if __name__ == '__main__':
    conf = SparkConf()
    ctx = SparkContext(master='local[*]', appName=APP_NAME, conf=conf)
    ctx.setLogLevel('INFO')

    rdd = create_pair_rdd(ctx)

    groupedRDD = rdd.groupByKey(numPartitions=4)
    sortedRDD = groupedRDD.mapValues(sort_group)

    lossRDD = sortedRDD.mapValues(calculate_loss)
    lossRDD.saveAsTextFile(OUTPUT_DIR.format(timestamp=TIMESTAMP))

    ctx.stop()
