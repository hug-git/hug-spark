package com.atguigu.sparkcore.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_Action {
    def main(args: Array[String]): Unit = {
        //创建SparkConf
        val conf: SparkConf = new SparkConf().setAppName("Spark05_Action$").setMaster("local[*]")
        //创建SparkContext
        val sc: SparkContext = new SparkContext(conf)
        
        reduceTest(sc)
        //countTest(sc)
        //takeTest(sc)
        
        //关闭SparkContext
        sc.stop()
    }
    
    def reduceTest(sc: SparkContext): Unit ={
        val valueRDD: RDD[String] = sc.makeRDD(Array("1", "2", "3", "4"), 2)
    
        println(valueRDD.reduce(_ + _))
        println(valueRDD.reduce(_ + _))
        println(valueRDD.reduce(_ + _))
        println(valueRDD.reduce(_ + _))
        println(valueRDD.reduce(_ + _))
    }
    
    def countTest(sc: SparkContext): Unit ={
        val valueRDD: RDD[String] = sc.makeRDD(Array("1", "2", "3", "4"), 2)
        println(valueRDD.count())
    }
    
    def firstTest(sc: SparkContext): Unit ={
        val valueRDD: RDD[String] = sc.makeRDD(Array("1", "2", "3", "4"), 2)
        println(valueRDD.first())
    }
    
    def takeTest(sc: SparkContext): Unit ={
        val valueRDD: RDD[String] = sc.makeRDD(Array("7", "3", "8", "4"), 2)
        println(valueRDD.take(3).mkString(","))
        println(valueRDD.takeOrdered(3).mkString(","))
    }
}
