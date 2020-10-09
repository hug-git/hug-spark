package com.atguigu.streaming.day03

import java.sql.Driver

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Spark01_WordCount_State {
    def main(args: Array[String]): Unit = {
        // 创建SparkConf
        val sparkConf: SparkConf = new SparkConf().setAppName("Spark01_WordCount_State$").setMaster("local[*]")
        
        // 初始化SparkSteamingContext
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        
        ssc.checkpoint("./ck2")
        val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",1234)
        
        val wordToOne: DStream[(String, Int)] = lineDStream.flatMap(_.split(" ")).map((_,1))
        
        val result: DStream[(String, Int)] = wordToOne.updateStateByKey(
            (seq: Seq[Int],state: Option[Int]) => {
                Some(seq.sum + state.getOrElse(0))
            })
        
        result.print()
        
        //启动SparkStreamingContext
        ssc.start()
        ssc.awaitTermination()
    }
}
