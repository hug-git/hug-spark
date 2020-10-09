package com.atguigu.sparkcore.day01

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {
    def main(args: Array[String]): Unit = {
        //创建SparkConf
        val conf: SparkConf = new SparkConf().setAppName("Spark01_WordCount").setMaster("local[*]")
        //创建SparkContext
        val sc = new SparkContext(conf)
        //读取数据
        //Path：可以是目录，也可以是具体的文件
        val line: RDD[String] = sc.textFile("./input")
        //扁平化操作
        val word: RDD[String] = line.flatMap(_.split(" "))
        //将每一个单词转换为元祖
        val wordToOne: RDD[(String, Int)] = word.map((_,1))
        //按照key做聚合操作
        val wordCount: RDD[(String, Int)] = wordToOne.reduceByKey(_+_)
        //打印
        println(wordCount.collect().sortBy(_._2)(Ordering[Int].reverse).mkString(","))
        //输出到文件
//        wordCount.saveAsTextFile(args(1))
        //关闭SparkContext
        sc.stop()
    }
}
