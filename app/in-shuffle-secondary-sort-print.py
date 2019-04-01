import uuid
import time
import os
import itertools
import re

from pyspark import SparkContext, SparkConf
from pyspark.rdd import portable_hash


APP_NAME = 'in-shuffle-secondary-sort-print'
INPUT_FILE = '/data/Taxi_Trips.csv.xsmall'
OUTPUT_DIR = '/data/output-in-shuffle-sort-print-{timestamp}.txt'
OUTPUT_FILE = OUTPUT_DIR + '/part-{identifier}'

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
    try:
        os.mkdir(OUTPUT_DIR.format(timestamp=TIMESTAMP))
    except Exception as e:
        print(e)

    identifier = uuid.uuid1()

    output_file = os.path.join(OUTPUT_FILE.format(timestamp=TIMESTAMP, identifier=str(identifier)))
    with open(output_file, 'w') as f:
        for k, g in itertools.groupby(lines, key=lambda x: x[0]):
            f.write(str(k) + ": ")
            for item in g:
                f.write('(' + str(item[1]) + ',' + str(item[2]) + ')')
            f.write('\n')


if __name__ == '__main__':
    conf = SparkConf()
    ctx = SparkContext(master='local[*]', appName=APP_NAME, conf=conf)
    ctx.setLogLevel('INFO')

    rdd = create_pair_rdd(ctx)

    sortedRDD = rdd.repartitionAndSortWithinPartitions(partitionFunc=partition_func,
                                                       numPartitions=4,
                                                       keyfunc=key_func,
                                                       ascending=True)
    unpairedRDD = sortedRDD.map(unpair, preservesPartitioning=True)
    unpairedRDD.foreachPartition(sorted_group)

    ctx.stop()
