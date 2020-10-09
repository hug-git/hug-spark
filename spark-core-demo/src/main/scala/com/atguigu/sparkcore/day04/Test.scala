package com.atguigu.sparkcore.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test{
    def main(args: Array[String]): Unit = {
        
        //创建SparkConf
        val conf: SparkConf = new SparkConf().setAppName("Test$").setMaster("local[*]")
        //创建SparkContext
        val sc: SparkContext = new SparkContext(conf)
        
        demo3(sc)
        
        //关闭SparkContext
        sc.stop()
        
    }
    
    def demo1(sc: SparkContext): Unit ={
        val arrs: RDD[Any] = sc.makeRDD(List(1,2,3,List(11,22,33),List(List(2,3))))
        
        val value1: RDD[Any] = arrs.flatMap {
            case list: List[_] => list.map{
                case list: List[_] => list
                case x: Int => List(x)
            }
            case value: Int => List(List(value))
        }.flatMap(list => list)
        
        println(value1.collect().mkString(","))
        
    }
    
    def demo2(sc: SparkContext): Unit ={
        val rdd: RDD[(Int,String)] = sc.makeRDD(List((1,"one"),(2,"two"),(3,"three"),(5,"five"),(6,"six")),2)
        val value: RDD[(String, Int)] = rdd.map(x => (x._2,x._1))
        println(value.reduceByKey(_ + _).partitioner.get)
        val value1: RDD[(Int, (Int, String))] = rdd.mapPartitionsWithIndex((index,iter) => iter.map((index,_)))
        value1.foreach(println)
        println(rdd.partitions.mkString(","))
    }
    
    def demo3(sc: SparkContext): Unit ={
        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,7),2)
        val i: Int = rdd.reduce(_+_)
        val index: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex((index,iter) => iter.map((index,_)))
        println(index.collect.mkString(","))
        
    }
}
