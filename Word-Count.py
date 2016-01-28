#count the number of unique words
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

lines = sc.textFile("C:/Users/seeth_000/UdemySpark/book.txt")
words = lines.flatMap(lambda x: x.split())
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore') #make sure we have UTF8 or unicode, ignore conversion errors
    if (cleanWord):
        print cleanWord, count

#!spark-submit Word-Count.py
