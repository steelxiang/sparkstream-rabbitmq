package stream

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.rabbitmq.RabbitMQUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * @author xiang
  *  2018/10/20
  */
object SparkMaster {
  def main(args: Array[String]): Unit = {
    //1、创建sparkConf
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("SparkStreamingKafka_Receiver")
      .setMaster("local[2]")
      //.set("spark.streaming.receiver.writeAheadLog.enable","true") //开启wal预写日志，保存数据源的可靠性
    //2、创建sparkContext
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    //3、创建StreamingContext
    val ssc = new StreamingContext(sc,Seconds(5))
    //设置checkpoint
    ssc.checkpoint("./checkpoint")
    val receiverStream: ReceiverInputDStream[String] = RabbitMQUtils.createStream(ssc, Map(

      "hosts" -> "47.104.198.254",

      "queueName" -> "hello",

      "exchangeName" -> "test",

      "exchangeType" -> "direct",
      "routingKeys" ->"xiang",

      "password" -> "admin",

      "userName" -> "admin",

      "vHost" -> "/"



    ))
    //val totalEvents = ssc.sparkContext.longAccumulator("My Accumulator")



    // Start up the receiver.

    receiverStream.start()



    // Fires each time the configured window has passed.






   // receiverStream.start()
    println("-------------------")
    receiverStream.print()
    val value: DStream[Long] = receiverStream.count()
    println("==============")
    value.print()
    println("***********")
    ssc.start() // Start the computation

    ssc.awaitTerminationOrTimeout(1000L) // Wait for the computation to terminate

  }
}
