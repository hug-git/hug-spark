package com.atguigu.streaming.day03

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Spark02_WordCount_Window01 {
    def main(args: Array[String]): Unit = {
        // 创建SparkConf
        val sparkConf: SparkConf = new SparkConf().setAppName("Spark02_WordCount_Window01$").setMaster("local[*]")
        
        // 初始化SparkSteamingContext
        val ssc = new StreamingContext(sparkConf, Seconds(3))
        
        val kafkaParams: Map[String, Object] = Map[String,Object](
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
            ConsumerConfig.GROUP_ID_CONFIG -> "group1",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
        )

        val lineDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String,String](Set("ss"), kafkaParams))

        val lineDStreamToStr: DStream[String] = lineDStream.map(_.value())
        val lineWindowDStream: DStream[String] = lineDStreamToStr.window(Seconds(12),Seconds(3))

        lineWindowDStream.flatMap(_.split(" "))
            .map((_,1))
            .reduceByKey(_+_)
            .print()
//        val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",1234)
//
//        val wordToOne: DStream[(String, Int)] = lineDStream.flatMap(_.split(" "))
//            .map((_, 1))
//        val wordWindowDStream: DStream[(String, Int)] = wordToOne.window(Seconds(12),Seconds(3))
//
//        wordWindowDStream.reduceByKey(_+_).print()
        

            
        
        
        //启动SparkStreamingContext
        ssc.start()
        ssc.awaitTermination()
    }
}
