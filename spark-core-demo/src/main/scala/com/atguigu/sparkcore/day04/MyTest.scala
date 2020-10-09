package com.atguigu.sparkcore.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MyTest {
    def main(args: Array[String]): Unit = {
        //创建SparkConf
        val conf: SparkConf = new SparkConf().setAppName("MyTest$").setMaster("local[*]")
        //创建SparkContext
        val sc: SparkContext = new SparkContext(conf)
        
        demo(sc)
        
        //关闭SparkContext
        sc.stop()
    }
    
    def demo(sc: SparkContext): Unit ={
        //读取数据
        val oriData: RDD[String] = sc.textFile("./input/agent.txt")
        //拆分每行数据
        val arr: RDD[Array[String]] = oriData.map(_.split(" "))
        //每行数据组合
        val perAd: RDD[((String, String), Int)] = arr.map(arr => ((arr(1),arr(4)),1))
        //计算点击次数
        val perAd1: RDD[((String, String), Int)] = perAd.reduceByKey(_+_)
        //以省份为id ((String, String), Int) => (String, (String, Int))
        val perAd2: RDD[(String, (String, Int))] = perAd1.map(
            x => (x._1._1, (x._1._2, x._2))
            )
        //将广告分组
        val perPro: RDD[(String, Iterable[(String, Int)])] = perAd2.groupByKey()
        //从大到小排序
        val perPro1: RDD[(String, List[(String, Int)])] = perPro.mapValues {
            iter => iter.toList.sortBy(_._2).reverse.take(3)
        }
        perPro1.collect().foreach(println)
    }
    
}
