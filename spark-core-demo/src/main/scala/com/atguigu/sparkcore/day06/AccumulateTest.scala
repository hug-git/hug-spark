package com.atguigu.sparkcore.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object AccumulateTest {
    def main(args: Array[String]): Unit = {
        //创建SparkConf
        val conf: SparkConf = new SparkConf().setAppName("AccumulateTest").setMaster("local[*]")
        //创建SparkContext
        val sc: SparkContext = new SparkContext(conf)
        
        test3(sc)
        
        //关闭SparkContext
        sc.stop()
    }
    
    def test1(sc: SparkContext): Unit = {
        val value: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
        
        val accum: LongAccumulator = sc.longAccumulator
        //        sc.register(accum)
        
        val value1: RDD[Int] = value.map(x => {
            accum.add(x)
            x * 2
        })
        value1.foreach(println)
        println(accum.value)
    }
    
    def test2(sc: SparkContext): Unit = {
        val value: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 2)
        
        val accum = new MyAccum
        sc.register(accum)
        
        value.foreach(println)
    }
    
    def test3(sc: SparkContext): Unit = {
        val value: RDD[String] = sc.textFile("./input/a.txt")
        val words: RDD[String] = value.flatMap(_.split(" "))
        
        val accum = new MyAccum1
        sc.register(accum)
        
        words.foreach(accum.add)
        accum.value.foreach(println)
    }
}

class MyAccum extends AccumulatorV2[Int, Int] {
    private var sum = 0
    
    override def isZero: Boolean = sum == 0
    
    override def copy(): AccumulatorV2[Int, Int] = new MyAccum
    
    override def reset(): Unit = sum = 0
    
    override def add(v: Int): Unit = sum += v
    
    override def merge(other: AccumulatorV2[Int, Int]): Unit = {
        this.sum += other.value
    }
    
    override def value: Int = sum
}

class MyAccum1 extends AccumulatorV2[String, mutable.Map[String, Int]] {
    private var map: mutable.Map[String, Int] = mutable.Map()
    
    override def isZero: Boolean = map.isEmpty
    
    override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
        new MyAccum1
    }
    
    override def reset(): Unit = map.clear
    
    override def add(v: String): Unit = {
        map(v) = map.getOrElse(v, 0) + 1
    }
    
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
        val map1: mutable.Map[String, Int] = map
        val map2: mutable.Map[String, Int] = other.value
        map = map1.foldLeft(map2) {
            (map, kv) => {
                map(kv._1) = map.getOrElse(kv._1, 0) + kv._2
                map
            }
        }
        //or
        //other.value.foreach(kv => map(kv._1) = map.getOrElse(kv._1, 0) + kv._2)
    }
    
    override def value: mutable.Map[String, Int] = map
}
