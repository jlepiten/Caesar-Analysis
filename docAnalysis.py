# import findspark
# import sys
# findspark.init()

import pyspark
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Midterm")
sc = SparkContext(conf = conf)

# Read .txt file and Create RDD
input = sc.textFile("./sample-3.txt")

# Total word and char count
words_total = 0
char_total = 0
with open("./sample-3.txt", 'r') as file:
    for line in file:
        wordsList = line.split()
        words_total += len(wordsList)
        for word in wordsList:
            char_total += len(word)

# Count each character
chars_counted = input.flatMap(lambda line: line) \
                     .map(lambda char: char) \
                     .map(lambda c: (c, 1)) \
                     .reduceByKey(lambda x,y: x + y)

# take top 10 character frequencies
sorted_chars_counted = chars_counted.map(lambda x: (x[1], x[0])) \
                                    .sortByKey(0) \
                                    .map(lambda x: (x[1], x[0]))

# output total counts to terminal
print("Word count: " + str(words_total) + "\nCharacter count: " + str(char_total))

# save sorted char counts as .txt file from
# highest to lowest frequencies
sorted_chars_counted.saveAsTextFile("./OUTPUTS")

# decryption of input file (caesar cypher)
