package com.atguigu.streaming08

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 手动维护Offset
 */
object KafkaDirectApiTest03 {
    def main(args: Array[String]): Unit = {
        // 创建SparkConf
        val sparkConf: SparkConf = new SparkConf().setAppName("KafkaDirectApiTest03$").setMaster("local[*]")
        
        // 初始化SparkSteamingContext
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        
        //使用DirectAPI消费Kafka数据创建流
        val kafkaParams: Map[String, String] = Map[String, String](
            "zookeeper.connect" -> "hadoop102:2181,hadoop103:2181,hadoop103:2181/kafka",
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
            ConsumerConfig.GROUP_ID_CONFIG -> "direct08"
        )
        
        val fromOffsets: Map[TopicAndPartition, Long] = Map[TopicAndPartition, Long](
            TopicAndPartition("ss", 0) -> 2L,
            TopicAndPartition("ss", 1) -> 4L
        )
        
        val lineDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc,
            kafkaParams,
            fromOffsets,
            (msg: MessageAndMetadata[String, String]) => (msg.key(), msg.message())
        )
        lineDStream.cache()
        
        //获取数据中的offset并打印
        lineDStream.foreachRDD { rdd =>
            val offsetRangs: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            offsetRangs.foreach(offsetRange => {
                println(s"${offsetRange.partition}-${offsetRange.fromOffset}-${offsetRange.untilOffset}")
            })
        }
        
        //计算WordCount并打印
        lineDStream.flatMap(_._2.split(" "))
            .map((_, 1))
            .reduceByKey(_ + _)
            .print()
        
        
        //启动SparkStreamingContext
        ssc.start()
        ssc.awaitTermination()
    }
}
