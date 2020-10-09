package com.atguigu.streaming.day01

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamWordCount {
    def main(args: Array[String]): Unit = {
        // 创建SparkConf
        val sparkConf: SparkConf = new SparkConf().setAppName("StreamWordCount").setMaster("local[*]")
        
        // 初始化SparkSteamingContext
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        
        //通过监控端口创建DStream，按行读取数据
        val lineStreams: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
        
        val wordStreams: DStream[String] = lineStreams.flatMap(_.split(" "))
        
        val wordCountStreams: DStream[(String, Int)] = wordStreams.map((_,1)).reduceByKey(_+_)
        
        wordCountStreams.print()
        
        //启动SparkStreamingContext
        ssc.start()
        ssc.awaitTermination()
    }
}
