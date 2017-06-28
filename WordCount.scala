import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

val textFile = sc.textFile("file///../../../Spark_Resources/input.txt")
val flatMapped = textFile.flatMap(_.split(" "))
val wordCounts = flatMapped.map((_,1)).reduceByKey(_ + _)

System.out.println(wordCounts.collect().mkString(", "))
