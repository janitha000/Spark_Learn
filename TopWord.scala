import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

val textFile = sc.textFile("file///../../../Spark_Resources/input.txt")
val tokenized = textFile.flatMap(x => x.split(" "))
val mapped = tokenized.map(x => (x,1))
val countPairs = mapped.reduceByKey(_ + _)

val swapped = countPairs.map(_.swap)
val sortedPairs = swapped.sortByKey(false,1)
System.out.println(sortedPairs.top(5).mkString(", "))
