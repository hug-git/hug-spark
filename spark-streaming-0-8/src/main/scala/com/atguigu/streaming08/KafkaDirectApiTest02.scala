package com.atguigu.streaming08

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaDirectApiTest02 {
    
    def getSSC: StreamingContext = {
        // 创建SparkConf
        val sparkConf: SparkConf = new SparkConf().setAppName("KafkaDirectApiTest02$").setMaster("local[*]")
    
        // 初始化SparkSteamingContext
        val ssc = new StreamingContext(sparkConf, Seconds(5))
    
        //使用DirectAPI消费Kafka数据创建流
        val kafkaParams: Map[String, String] = Map[String, String](
            "zookeeper.connect" -> "hadoop102:2181,hadoop103:2181,hadoop103:2181/kafka",
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
            ConsumerConfig.GROUP_ID_CONFIG -> "direct08"
            )
    
        val lineDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,Set("ss"))
    
        lineDStream.flatMap(_._2.split(" "))
            .map((_,1))
            .reduceByKey(_+_)
            .print()
        
        ssc
    }
    
    def main(args: Array[String]): Unit = {
        
        val ssc: StreamingContext = StreamingContext.getActiveOrCreate("./ck1", () => getSSC)
        
        //启动SparkStreamingContext
        ssc.start()
        ssc.awaitTermination()
    }
}
