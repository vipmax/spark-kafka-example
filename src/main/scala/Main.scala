import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Duration, StreamingContext, Time}

/**
  * Created by vipmax on 02.03.17.
  */
object Main {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[*]", "test")
    val ssc = new StreamingContext(sc, Duration(1000))


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics = Array("test"), kafkaParams = kafkaParams)
    )

    stream
      .map(record => (record.key, record.value))
      .foreachRDD((rdd: RDD[(String, String)], time: Time) => println("size", rdd.collect().size, "time",time))

    ssc.start()

    sc.addSparkListener(new SparkListener {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
        val metrics = taskEnd.taskMetrics

        println(taskEnd.stageId)
        println("input = " + metrics.inputMetrics.recordsRead)
        println("shuffle input = " + metrics.shuffleReadMetrics.recordsRead)
        println("shuffle output = " + metrics.shuffleWriteMetrics.recordsWritten)
        println("output = " + metrics.outputMetrics.recordsWritten)
      }
    })

    ssc.awaitTermination()

  }
}
