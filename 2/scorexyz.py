# For example, suppose you were given the following strategy guide:
#
# A Y
# B X
# C Z
# This strategy guide predicts and recommends the following:
#
# In the first round, your opponent will choose Rock (A), and you should choose Paper (Y). This ends in a win for you with a score of 8 (2 because you chose Paper + 6 because you won).
# In the second round, your opponent will choose Paper (B), and you should choose Rock (X). This ends in a loss for you with a score of 1 (1 + 0).
# The third round is a draw with both players choosing Scissors, giving you a score of 3 + 3 = 6.
# In this example, if you were to follow the strategy guide, you would get a total score of 15 (8 + 1 + 6).
#
# What would your total score be if everything goes exactly according to your strategy guide?

from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

spark = SparkSession \
    .builder \
    .appName("Advent Code 2022 Day 2-1") \
    .getOrCreate()


matches = spark.read.text('input.txt')
matches = matches.withColumn('index', monotonically_increasing_id())
points = spark.sparkContext.accumulator(0)


def getPointsForSecond(first, second):
    current = 0
    if ((first == 'A') and (second == 'Y')) \
    or ((first == 'B') and (second == 'Z')) \
    or ((first == 'C') and (second == 'X')):
        current += 6

    if ((first == 'A') and (second == 'X')) \
    or ((first == 'B') and (second == 'Y')) \
    or ((first == 'C') and (second == 'Z')):
        current += 3

    if second == 'X':
        current += 1
    elif second == 'Y':
        current += 2
    else:
        current += 3

    return current


def getResultsFromSecond(first, second):
    current = 0

    if ((first == 'A') and (second == 'Y')) \
    or ((first == 'B') and (second == 'X')) \
    or ((first == 'C') and (second == 'Z')):
        current += 1

    if ((first == 'A') and (second == 'Z')) \
    or ((first == 'B') and (second == 'Y')) \
    or ((first == 'C') and (second == 'X')):
        current += 2

    if ((first == 'A') and (second == 'X')) \
    or ((first == 'B') and (second == 'Z')) \
    or ((first == 'C') and (second == 'Y')):
        current += 3

    if second == 'Z':
        current += 6
    elif second == 'Y':
        current += 3
    else:
        current += 0

    return current


def f(value, p):
    choices = value['value'].split()
    currentPoints = getPointsForSecond(choices[0], choices[1])
    p.add(currentPoints)


def g(value, p):
    choices = value['value'].split()
    currentPoints = getResultsFromSecond(choices[0], choices[1])
    p.add(currentPoints)


# matches.foreach(lambda x: f(x, points))
matches.foreach(lambda x: g(x, points))

print('final points are:' + str(points.value))
