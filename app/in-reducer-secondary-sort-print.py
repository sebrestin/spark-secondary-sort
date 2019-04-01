import time
import os
import re
import uuid
from pyspark import SparkContext, SparkConf


APP_NAME = 'in-reduce-secondary-sort-print'
INPUT_FILE = '/data/Taxi_Trips.csv.xsmall'
OUTPUT_DIR = '/data/output-in-reduce-sort-print-{timestamp}.txt'
OUTPUT_FILE = OUTPUT_DIR + '/part-{identifier}'

COMMA_DELIMITER = re.compile(''',(?=(?:[^"]*"[^"]*")*[^"]*$)''')

FIRST_KEY = 16
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


def save_as_sorted_group(lines):
    try:
        os.mkdir(OUTPUT_DIR.format(timestamp=TIMESTAMP))
    except Exception as e:
        print(e)

    identifier = uuid.uuid1()

    output_file = os.path.join(OUTPUT_FILE.format(timestamp=TIMESTAMP, identifier=str(identifier)))
    with open(output_file, 'w') as f:

        for line in lines:
            k = line[0]
            g = line[1]
            f.write(str(k) + ": ")
            for item in g:
                f.write('(' + str(item[0]) + ',' + str(item[1]) + ')')
            f.write('\n')


if __name__ == '__main__':
    conf = SparkConf()
    ctx = SparkContext(master='local[*]', appName=APP_NAME, conf=conf)
    ctx.setLogLevel('INFO')

    rdd = create_pair_rdd(ctx)

    groupedRDD = rdd.groupByKey(numPartitions=4)
    sortedRDD = groupedRDD.mapValues(sort_group)

    sortedRDD.foreachPartition(save_as_sorted_group)

    ctx.stop()
