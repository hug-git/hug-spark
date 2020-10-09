package com.atguigu.sparkcore.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_Transform {
    def main(args: Array[String]): Unit = {
        //创建SparkConf
        val conf: SparkConf = new SparkConf().setAppName("Spark04_Transform$").setMaster("local[*]")
        //创建SparkContext
        val sc: SparkContext = new SparkContext(conf)
        
        //aggregateByKeyTest(sc)
        //foldByKeyTest(sc)
        //combineByKeyTest(sc)
        //sortByKeyTest(sc)
        joinTest(sc)
        //coGroupTest(sc)
        //mapValuesTest(sc)
        //flatMapValuesTest(sc)
        
        //关闭SparkContext
        sc.stop()
    }
    
    //1.初始值，区内计算逻辑，区间计算逻辑
    def aggregateByKeyTest(sc: SparkContext): Unit = {
        val kvRDD: RDD[(String, Int)] = sc.makeRDD(List(
            ("a", 3), ("a", 2), ("c", 4),
            ("b", 3), ("c", 6), ("c", 8)
            ), 2)
        //区内最大值，区间求和
        kvRDD.aggregateByKey(0)(
            (x, y) => Math.max(x, y),
            _ + _
            ).collect().foreach(println)
    }
    
    //2.初始值，区内及区间计算逻辑
    def foldByKeyTest(sc: SparkContext): Unit = {
        val kvRDD: RDD[(String, Int)] = sc.makeRDD(List(
            ("a", 3), ("a", 2), ("c", 4),
            ("b", 3), ("c", 6), ("c", 8)
            ), 2)
        
        //WordCount
        kvRDD.foldByKey(0)(_ + _).foreach(println)
    }
    
    //3.第一个值变换逻辑，区内计算逻辑，区间计算逻辑
    def combineByKeyTest(sc: SparkContext): Unit = {
        val kvRDD: RDD[(String, Int)] = sc.makeRDD(List(
            ("a", 3), ("a", 2), ("c", 4),
            ("b", 3), ("c", 6), ("c", 8)
            ), 2)
        //区内求sum，count，区间求avg
        kvRDD.combineByKey(
            (x: Int) => (x, 1),
            (x: (Int, Int), y: Int) => (x._1 + y, x._2 + 1),
            (a: (Int, Int), b: (Int, Int)) => (a._1 + b._1, a._2 + b._2)
            ).map {
            case (word, (sum, count)) => (word, sum / count)
        }.collect().foreach(println)
    }
    
    //4.combineByKey与aggregateByKey需求对调
    def test(sc: SparkContext): Unit = {
        val kvRDD: RDD[(String, Int)] = sc.makeRDD(List(
            ("a", 3), ("a", 2), ("c", 4),
            ("b", 3), ("c", 6), ("c", 8)
            ), 2)
        
        kvRDD.combineByKey(
            (x: Int) => x,
            (x: Int, y: Int) => Math.max(x, y),
            (a: Int, b: Int) => a + b
            ).foreach(println)
        
        kvRDD.aggregateByKey((0, 0))(
            (x: (Int, Int), y: Int) => (x._1 + y, x._2 + 1),
            (a: (Int, Int), b: (Int, Int)) => (a._1 + b._1, a._2 + b._2)
            ).collect().foreach(println)
    }
    
    //5.根据key值排序
    def sortByKeyTest(sc: SparkContext): Unit = {
        val kvRDD: RDD[(String, Int)] = sc.makeRDD(List(
            ("a", -3), ("e", -2), ("c", -4),
            ("b", -3), ("d", -6), ("c", -8)
            ), 2)
        kvRDD.sortByKey().collect().foreach(println)
    }
    
    //6.内连接、左外连接、右外连接、满外连接
    def joinTest(sc: SparkContext): Unit = {
        val kvRDD1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 1), ("a", 2), ("b", 1), ("d", 1)), 2)
        val kvRDD2: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 1), ("c", 1)), 2)
        
        val join: RDD[(String, (Int, Int))] = kvRDD1.join(kvRDD2)
        val leftJoin: RDD[(String, (Int, Option[Int]))] = kvRDD1.leftOuterJoin(kvRDD2)
        val rightJoin: RDD[(String, (Option[Int], Int))] = kvRDD1.rightOuterJoin(kvRDD2)
        val fullJoin: RDD[(String, (Option[Int], Option[Int]))] = kvRDD1.fullOuterJoin(kvRDD2)
        
        println("============join=============")
        join.collect().foreach(println)
        println("============left-join=============")
        leftJoin.collect().foreach(println)
        println("============right-join=============")
        rightJoin.collect().foreach(println)
        println("============full-join=============")
        fullJoin.collect().foreach(println)
    }
    
    //7.两个RDD分组 kvRDD1,kvRDD2 --> (key,(CompactBuffer(v1,v2,...),CompactBuffer(v1,v2,...))
    def coGroupTest(sc: SparkContext): Unit = {
        val kvRDD1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 1), ("d", 1)), 2)
        val kvRDD2: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 1), ("c", 1)), 2)
        
        val coGroup: RDD[(String, (Iterable[Int], Iterable[Int]))] = kvRDD1.cogroup(kvRDD2)
        coGroup.collect().foreach(println)
    }
    
    //8.对(k,v)对的value进行map操作
    def mapValuesTest(sc: SparkContext): Unit = {
        val kvRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 1), ("b", 1), ("d", 1)), 2)
        val mv: RDD[(String, Int)] = kvRDD.mapValues(_ * 3)
        mv.collect().foreach(println)
        
    }
    //9.列转行操作
    def flatMapValuesTest(sc: SparkContext): Unit = {
        val kvRDD: RDD[(String, Array[Int])] = sc.makeRDD(List(("a", Array(1, 2, 3)), ("b", Array(1, 2))), 2)
        
        val fm: RDD[(String, Int)] = kvRDD.flatMap {
            case (value, arr) => arr.map((value, _))
        }
        fm.collect().foreach(println)
        println("===========================")
        val fmv: RDD[(String, Int)] = kvRDD.flatMapValues(x => x)
        fmv.collect().foreach(println)
    }
    
}
