package com.atguigu.streaming.day03

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Spark03_WordCount_Window02 {
    def main(args: Array[String]): Unit = {
        // 创建SparkConf
        val sparkConf: SparkConf = new SparkConf().setAppName("Spark03_WordCount_Window02$").setMaster("local[*]")
        
        // 初始化SparkSteamingContext
        val ssc = new StreamingContext(sparkConf, Seconds(3))
        ssc.checkpoint("./ck3")
        val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",1234)
        
        val wordToOne: DStream[(String, Int)] = lineDStream.flatMap(_.split(" ")).map((_,1))
        
        wordToOne.reduceByKeyAndWindow(
            (x:Int,y:Int) => x + y,
            (a:Int,b:Int) => a - b,
            Seconds(12),
            Seconds(3),
            new HashPartitioner(2),
            (kv:(String, Int)) => kv._2 > 0
        ).print()
        
        //优雅关闭
//        new Thread(new MonitorStop(ssc)).start()
        //启动SparkStreamingContext
        ssc.start()
        ssc.awaitTermination()
    }
}
