from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf = conf)

lines = sc.textFile("C:/Users/seeth_000/UdemySpark/ml-100k/u.data")
movies = lines.map(lambda x: (int(x.split()[1]), 1))
movieCounts = movies.reduceByKey(lambda x, y: x + y)

flipped = movieCounts.map( lambda (x, y) : (y, x) )
sortedMovies = flipped.sortByKey()

results = sortedMovies.collect()

for result in results:
    print result

#!spark-submit Popular-Movies.py
