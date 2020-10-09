package com.atguigu.sparkcore.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark03_Transform {
    def main(args: Array[String]): Unit = {
        //创建SparkConf
        val conf: SparkConf = new SparkConf().setAppName("Spark03_Transform$").setMaster("local[*]")
        //创建SparkContext
        val sc: SparkContext = new SparkContext(conf)
        
        //coalesceTest(sc)
        //repartitionTest(sc)
        //sortByTest(sc)
        //unionTest(sc)
        //partitionByTest(sc)
        //reduceByKeyTest(sc)
        groupByKeyTest(sc)
        
        //关闭SparkContext
        sc.stop()
    }
    
    //1.缩减分区
    def coalesceTest(sc: SparkContext): Unit ={
        val valueRDD1: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5, 6, 7, 8), 4)
        val valueRDD2: RDD[Int] = valueRDD1.coalesce(2,true)//true表示使用shuffle
        println("缩减后的分区数：" + valueRDD2.getNumPartitions)
        
        //缩减前后分区数据
        val res1: RDD[(Int, Int)] = valueRDD1.mapPartitionsWithIndex((index,iter) => iter.map((index,_)))
        println(res1.collect().mkString(","))
        val res2: RDD[(Int, Int)] = valueRDD2.mapPartitionsWithIndex((index,iter) => iter.map((index,_)))
        println(res2.collect().mkString(","))
        
    }
    
    //2.扩大分区
    def repartitionTest(sc: SparkContext): Unit ={
        val valueRDD1: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5, 6, 7, 8), 4)
        valueRDD1.mapPartitionsWithIndex((index, iter) => iter.map((index, _))).collect().foreach(println)
        println("================")
        val valueRDD2: RDD[Int] = valueRDD1.repartition(8)
        valueRDD2.mapPartitionsWithIndex((index, iter) => iter.map((index, _))).collect().foreach(println)
        
    }
    
    //3.排序
    def sortByTest(sc: SparkContext): Unit ={
        val valueRDD: RDD[Int] = sc.makeRDD(Array(1, 5, 3, 4, 2, 6, 9, 8), 4)
        valueRDD.sortBy(x => x).collect().foreach(println)
    }
    
    //4.交集、并集、差集
    def unionTest(sc: SparkContext): Unit ={
        val valueRDD1: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4), 2)
        val valueRDD2: RDD[Int] = sc.makeRDD(Array(3, 4, 5, 6), 2)
        
        //交集
        println(valueRDD1.union(valueRDD2).collect().mkString(","))
        //并集
        println(valueRDD1.intersection(valueRDD2).collect().mkString(","))
        //差集
        println(valueRDD1.subtract(valueRDD2).collect().mkString(","))
        
        //拉链（要求两者分区数及分区中的元素个数一样）
        println(valueRDD1.zip(valueRDD2).collect().mkString(","))
        
    }
    
    //5.根据分区器重新分区
    def partitionByTest(sc: SparkContext): Unit = {
        val kvRDD: RDD[(String, Int)] = sc.makeRDD(Array(("a", 1), ("b", 1), ("c", 1), ("d", 1)), 2)
        kvRDD.mapPartitionsWithIndex((index,iter) => iter.map((index,_))).collect().foreach(println)
        println("=====重新分区后======")
        kvRDD.partitionBy(new HashPartitioner(2))
            .mapPartitionsWithIndex(
                (index,iter) => iter.map((index,_))
            ).collect().foreach(println)
    }
    
    //6.聚合
    def reduceByKeyTest(sc: SparkContext): Unit ={
        val kvRDD1: RDD[(String, Int)] = sc.makeRDD(Array(("a", 1), ("b", 1), ("a", 1), ("d", 1)), 2)
        println(kvRDD1.reduceByKey(_ + _).partitioner.get)
        kvRDD1.reduceByKey(_+_).collect().foreach(println)
    }
    
    //7.Group by key
    def groupByKeyTest(sc: SparkContext): Unit ={
        val kvRDD1: RDD[(String, Int)] = sc.makeRDD(Array(("a", 1), ("b", 1), ("a", 1), ("d", 1)), 2)
        val value: RDD[(String, Iterable[Int])] = kvRDD1.groupByKey()
    
        println(value.collect().mkString(","))
        
        value.map {
            case (value, iter) => (value, iter.size)
        }.collect().foreach(println)
    }
}
