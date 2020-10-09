package com.atguigu.sparkcore.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Transform {
    def main(args: Array[String]): Unit = {
        //创建SparkConf
        val conf: SparkConf = new SparkConf().setAppName("Spark01_Transform").setMaster("local[*]")
        //创建SparkContext
        val sc: SparkContext = new SparkContext(conf)
        
//        mapTest(sc)
//        mapPartitionsTest(sc)
//        mapPartitionsWithIndexTest(sc)
        flatMapTest(sc)
//        glomTest(sc)
        
        //关闭SparkContext
        sc.stop()
    }
    
    //1.对元素进行map映射操作
    def mapTest(sc: SparkContext): Unit ={
        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5))
        println(rdd.map(_ * 2).collect().mkString(","))
        println(rdd.map(_ + "-").collect().mkString(","))
    }
    
    //2.针对每个分区进行map映射操作
    def mapPartitionsTest(sc: SparkContext): Unit ={
        val dataRDD: RDD[Int] = sc.makeRDD(List(1,3,6,2,5,7,8),2)
        
        //每个数字x2
        val mul: RDD[Int] = dataRDD.mapPartitions(_.map(_*2))
        println(mul.collect().mkString("-"))
        
        //过滤
        val value: RDD[Int] = dataRDD.mapPartitions(
            datas =>
                datas.filter(_%2 == 0)
            )
        println(value.collect().mkString(" "))
        
        //获取每个分区最大值
        val rdd: RDD[Int] = dataRDD.mapPartitions(
            iter =>
                List(iter.max).iterator
            )
        println(rdd.collect().mkString(","))
    }
    
    //3.带分区号的分区map映射操作
    def mapPartitionsWithIndexTest(sc: SparkContext): Unit ={
        val dataRDD: RDD[Int] = sc.makeRDD(List(1,3,6,2,5,7,8),2)
        val kv: RDD[(Int, Int)] = dataRDD.mapPartitionsWithIndex((index, iter) => {
            iter.map((index, _))
        })
        println(kv.collect().mkString(" "))
    }
    
    //4.扁平化数据集
    def flatMapTest(sc: SparkContext): Unit ={
        val anyRDD: RDD[Any] = sc.makeRDD(List(List(1, 2), 3, List(4, 5)), 1)
//        val rdd0: RDD[Any] = anyRDD.flatMap(elem => {
//            elem match {
//                case list: List[_] => list
//                case value: Int => List(value)
//            }
//        })
//        println(rdd0.collect().mkString(","))
    
        val rdd1: RDD[Any] = anyRDD.flatMap {
            case list: List[_] => list
            case value: Int => List(value)
        }
        println(rdd1.collect().mkString(","))
    }
    
    //5.以数组形式返回单位分区的所有元素
    def glomTest(sc: SparkContext): Unit ={
        val value: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5, 6), 2)
        //将每个分区中的所有元素放入一个数组返回
        val arrs: RDD[Array[Int]] = value.glom()
        arrs.collect().foreach(
            arr =>
            println(arr.mkString(","))
        )
        
        println("---Max---")
        //每个分区最大值
        val max: RDD[Int] = value.glom().map(_.max)
        println(max.collect().mkString(","))
    }
    
}
