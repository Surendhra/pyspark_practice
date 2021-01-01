from pyspark import SparkConf
from pyspark import SparkContext
import logging
logging.basicConfig(filename='/Users/surendhra.ganji1@ibm.com/Documents/surendhra/Spark/logs/python.log',level=logging.ERROR,filemode='w')
def main():

 try:

  spark1 = SparkConf().setMaster("local[*]").setAppName("rdd logging")
  sc = SparkContext(conf=spark1)
  sc._jvm.org.apache.log4j.Logger
  file1=sc.textFile("/Users/surendhra.ganji1@ibm.com/Documents/surendhra/Spark/rawfiles/sampledat.txt").flatMap(lambda f: f.split(" "))\
    .map(lambda x: (x,1)).reduceByKey(lambda x,y: (x+y))
  print(file1.collect())
  except Exception as e:
  logging.error("error in spark code")

 finally: sc.stop()
 #

if __name__ == '__main__':
    main()