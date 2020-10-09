package com.atguigu.sparkcore.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_CreateRddByArray {
    def main(args: Array[String]): Unit = {
        //创建SparkConf
        val conf: SparkConf = new SparkConf().setAppName("Spark01_CreateRddByArray$").setMaster("local[*]")
        //创建SparkContext
        val sc: SparkContext = new SparkContext(conf)
//        val rdd: RDD[Int] = sc.parallelize(Array(1,2,3,4,5))
        val rdd: RDD[Int] = sc.makeRDD(Array(1,2,3,4,5))
        
        rdd.map(_*2).collect().foreach(println)
        //关闭SparkContext
        sc.stop()
    }
}
