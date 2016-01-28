#COunting the number of unique ratings
#spark context allows you to create RDD
#spark conf : setup the sparkcontext, local vs cluster etc
from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("C:/Users/seeth_000/UdemySpark/ml-100k/u.data") #creating RDD
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue() #creates tuples of ratings and #unique val count

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.iteritems():
    print "%s %i" % (key, value)

#spark-submit Ratings-Counter.py