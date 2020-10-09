package com.atguigu.sparkcore.day06

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object BroadcastTest {
    def main(args: Array[String]): Unit = {
        //创建SparkConf
        val conf: SparkConf = new SparkConf().setAppName("BroadcastTest$").setMaster("local[*]")
        //创建SparkContext
        val sc: SparkContext = new SparkContext(conf)
        
        val value: RDD[Int] = sc.makeRDD(Array(1,2,3,4,5,6,7,8,9),2)
        
        //过滤对象
        val ints = Array(2,4,6)
        val intsBC: Broadcast[Array[Int]] = sc.broadcast(ints)
        
        //过滤
        val value1: RDD[Int] = value.filter(intsBC.value.contains)
        
        value1.collect.foreach(println)
        
        //关闭SparkContext
        sc.stop()
    }
}
