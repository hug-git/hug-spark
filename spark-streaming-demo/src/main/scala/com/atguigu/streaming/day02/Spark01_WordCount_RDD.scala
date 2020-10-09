package com.atguigu.streaming.day02

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object Spark01_WordCount_RDD {
    def main(args: Array[String]): Unit = {
        // 创建SparkConf
        val sparkConf: SparkConf = new SparkConf().setAppName("Spark01_WordCount_RDD$").setMaster("local[*]")
        
        // 初始化SparkSteamingContext
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        
        val queue = new mutable.Queue[RDD[Int]]()
        
        val rddDStream: InputDStream[Int] = ssc.queueStream(queue, oneAtATime = false)
        
        val sumDStream: DStream[Int] = rddDStream.reduce(_+_)
        
        sumDStream.print()
        
        //启动SparkStreamingContext
        ssc.start()
        
        //向RDD队列中添加数据
        for (i <- 1 to 5) {
            queue += ssc.sparkContext.makeRDD(1 to 100, 10)
            Thread.sleep(2000)
        }
        
        ssc.awaitTermination()
    }
}
