import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{ Seconds, StreamingContext }

object TwitterTags{
  def main(){
    System.setProperty("twitter4j.oauth.consumerKey", "tRXsnnmFCRM44sW1OWxBEPTqn")
    System.setProperty("twitter4j.oauth.consumerSecret", "PNCOQUbWTYokDsy0LqfysxxxgzTeFHbnxwECYtuTQmyET8P5BW")
    System.setProperty("twitter4j.oauth.accessToken", "	84753528-PkcHOlft7btVtCmDSORAVD9rbM3QCFUa0RqGU6lTs")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "c8sY8jC2L3bZ0zvzEog76v1BrrTVDRwTpLJefjFkimSEr")

    val SparkConf = new SparkConf().setAppName("TwitterTags").setMaster("local[2]")
    val ssc = new StreamingContext(SparkConf, 5)
    val stream = TwitterUtils.createStream(ssc, None)

    val HashTags = stream.flatMap(x => x.getText.split(" ").filter(_.startsWith("#")))

    val top10 = HashTags.map((_ , 1)).reduceByKeyAndWindow(_ + _, Seconds(60)).map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

      top10.foreachRDD(rdd => {
         val topList = rdd.take(10)
         println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
         topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
       })

       ssc.start()
       ssc.awaitTermination()
  }
}
