package com.atguigu.sparkcore.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Transform {
    def main(args: Array[String]): Unit = {
        //创建SparkConf
        val conf: SparkConf = new SparkConf().setAppName("Spark02_Transform").setMaster("local[*]")
        //创建SparkContext
        val sc: SparkContext = new SparkContext(conf)
        
        //groupByTest(sc)
        //filterTest(sc)
        //sampleTest(sc)
        distinctTest(sc)
        
        //关闭SparkContext
        sc.stop()
    }
    
    //1.按自定义逻辑进行分组
    def groupByTest(sc: SparkContext): Unit ={
        val value: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5, 6, 5, 6, 5, 6, 8, 9, 1), 2)
        val group: RDD[(Int, Iterable[Int])] = value.groupBy(_%2)
        println(group.collect().mkString(","))
        
        //计算WordCount
        val wc: RDD[(Int, Int)] = value.groupBy(x => x).map{ case (value,iter) => (value,iter.size) }
        println(wc.collect().mkString(" "))
    }
    
    //2.过滤
    def filterTest(sc: SparkContext): Unit ={
        val value: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5, 6), 2)
        val filterNum: RDD[Int] = value.filter(_%2 == 0)
        println(filterNum.collect().mkString(","))
    }
    
    //3.抽样
    def sampleTest(sc: SparkContext): Unit ={
        val value: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 2)
        //不放回
        val noBack: RDD[Int] = value.sample(false,0.3)
        println(noBack.collect().mkString(","))
        //放回
        val withBack: RDD[Int] = value.sample(true,2)
        println(withBack.collect().mkString(","))
        
    }
    
    //4.去重
    def distinctTest(sc: SparkContext): Unit ={
        val value: RDD[Int] = sc.makeRDD(Array(1, 2, 10, 4, 5, 2, 7, 4, 9, 10), 2)
        val dist: RDD[Int] = value.distinct()
        println(dist.collect().mkString(","))
    }
    
}
