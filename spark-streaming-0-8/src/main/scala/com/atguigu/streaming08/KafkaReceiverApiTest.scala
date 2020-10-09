package com.atguigu.streaming08

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

object KafkaReceiverApiTest {
    def main(args: Array[String]): Unit = {
        // 创建SparkConf
        val sparkConf: SparkConf = new SparkConf().setAppName("KafkaReceiveApiTest$").setMaster("local[*]")
        
        // 初始化SparkSteamingContext
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        
        //使用ReceiverAPI消费Kafka数据创建流
        val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc,
                "hadoop102:2181,hadoop103:2181,hadoop103:2181/kafka",
                "receiver",
                Map[String, Int]("ss" -> 2))
        kafkaDStream.flatMap(_._2.split(" "))
                .map((_,1))
                .reduceByKey(_+_)
                .print()
        
        //启动SparkStreamingContext
        ssc.start()
        ssc.awaitTermination()
    }
}
