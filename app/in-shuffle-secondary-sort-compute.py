import time
import itertools
import re

from pyspark import SparkContext, SparkConf
from pyspark.rdd import portable_hash
from datetime import datetime

APP_NAME = 'in-shuffle-secondary-sort-compute'
INPUT_FILE = '/data/Taxi_Trips.csv.xsmall'
OUTPUT_DIR = '/data/output-in-shuffle-sort-compute-{timestamp}.txt'

COMMA_DELIMITER = re.compile(''',(?=(?:[^"]*"[^"]*")*[^"]*$)''')

FIRST_KEY = 1
SECOND_KEY = 2
TRIP_END_TIMESTAMP = 3


TIMESTAMP = int(time.time())


def partition_func(key):
    return portable_hash(key[0])


def key_func(entry):
    return entry[0], entry[1]


def make_pair(entry):
    key = (entry[FIRST_KEY], entry[SECOND_KEY])
    return key, entry


def unpair(entry):
    return entry[0][0], entry[1][0], entry[1][1]


def create_pair_rdd(ctx):
    rawRDD = ctx.textFile(INPUT_FILE)
    headerlessRDD = rawRDD.filter(lambda x: not x.startswith('Trip ID'))
    rdd = headerlessRDD.map(lambda x: COMMA_DELIMITER.split(x))
    validRDD = rdd.filter(lambda x: len(x[FIRST_KEY]) > 0 and len(x[SECOND_KEY]) > 0 and len(x[TRIP_END_TIMESTAMP]) > 0)
    pairRDD = validRDD.map(make_pair)
    compressedRDD = pairRDD.mapValues(lambda x: (x[SECOND_KEY], x[TRIP_END_TIMESTAMP]))

    return compressedRDD


def sorted_group(lines):
    return itertools.groupby(lines, key=lambda x: x[0])


def calculate_loss(entry):
    key, group = entry
    loss = 0
    _, _, prev_end = next(group)

    for item in group:
        _, start, end = item
        delta = datetime.strptime(start, '%m/%d/%Y %I:%M:%S %p').timestamp() \
                - datetime.strptime(prev_end, '%m/%d/%Y %I:%M:%S %p').timestamp()
        if delta > 0:
            loss += delta
        prev_end = end

    return key, loss


if __name__ == "__main__":
    conf = SparkConf()
    ctx = SparkContext(master="local[*]", appName=APP_NAME, conf=conf)
    ctx.setLogLevel('INFO')

    rdd = create_pair_rdd(ctx)

    sortedRDD = rdd.repartitionAndSortWithinPartitions(partitionFunc=partition_func,
                                                       numPartitions=4,
                                                       keyfunc=key_func,
                                                       ascending=True)
    unpairedRDD = sortedRDD.map(unpair, preservesPartitioning=True)
    groupedRDD = unpairedRDD.mapPartitions(sorted_group, preservesPartitioning=True)

    lossRDD = groupedRDD.map(calculate_loss)
    lossRDD.saveAsTextFile(OUTPUT_DIR.format(timestamp=TIMESTAMP))

    ctx.stop()
