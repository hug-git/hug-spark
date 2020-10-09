package com.atguigu.sparkcore.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_Action {
    def main(args: Array[String]): Unit = {
        //创建SparkConf
        val conf: SparkConf = new SparkConf().setAppName("Spark06_Action$").setMaster("local[*]")
        //创建SparkContext
        val sc: SparkContext = new SparkContext(conf)
        
        //aggregateTest(sc)
        //countByKey(sc)
        //foreachPartition(sc)
        //lineageTest(sc)
        dependenciesTest(sc)
        
        //关闭SparkContext
        sc.stop()
    }
    
    def aggregateTest(sc: SparkContext): Unit = {
        val valueRDD: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4), 4)
        println(valueRDD.aggregate(1)(_ + _, _ + _))
        println(valueRDD.fold(1)(_ + _))
    }
    
    def aggPractice(sc: SparkContext): Unit ={
        //3.创建RDD
        val rdd: RDD[String] = sc.makeRDD(Array("12", "234", "345", "4567"), 2)
        // 034/043
        println(rdd.aggregate("0")((x, y) => math.max(x.length, y.length).toString, (a, b) => a + b))
        // 11
        println(rdd.aggregate("")((x, y) => math.min(x.length, y.length).toString, (a, b) => a + b))
    }
    
    def countByKey(sc: SparkContext): Unit ={
        val kvRDD: RDD[(String, Int)] = sc.makeRDD(Array(("a", 1), ("a", 2), ("b", 1),
                                                         ("a", 3), ("b", 4), ("b", 5)), 2)
        println(kvRDD.countByKey())
        
    }
    
    def foreachPartition(sc: SparkContext): Unit ={
        val kvRDD: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4), 2)
        
        kvRDD.foreachPartition(
            iter => println(iter.mkString(","))
        )
        kvRDD.foreach(println)
        println("================")
        kvRDD.foreach(println)
        println("================")
        kvRDD.foreach(println)
    }
    
    /**
     * 血统
     * @param sc SparkContext
     */
    def lineageTest(sc: SparkContext): Unit ={
        val valueRDD: RDD[String] = sc.textFile("./input/a.txt")
        println(valueRDD.toDebugString)
        println("==============================")
        val word: RDD[String] = valueRDD.flatMap(_.split(" "))
        println(word.toDebugString)
        println("==============================")
        val word_one: RDD[(String, Int)] = word.map((_,1))
        println(word_one.toDebugString)
        println("==============================")
        val wordCount: RDD[(String, Int)] = word_one.reduceByKey(_+_)
        println(wordCount.toDebugString)
        
    }
    
    /**
     * 依赖关系
     * @param sc SparkContext
     */
    def dependenciesTest(sc: SparkContext): Unit ={
        val valueRDD: RDD[String] = sc.textFile("./input/a.txt")
        println(valueRDD.dependencies)
        println("==============================")
        val word: RDD[String] = valueRDD.flatMap(_.split(" "))
        println(word.dependencies)
        println("==============================")
        val word_one: RDD[(String, Int)] = word.map((_,1))
        println(word_one.dependencies)
        println("==============================")
        val wordCount: RDD[(String, Int)] = word_one.reduceByKey(_+_)
        println(wordCount.dependencies)
    }
}
