package com.atguigu.sparkcore.day05

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object KryoTest {
    def main(args: Array[String]): Unit = {
        //创建SparkConf
        val conf: SparkConf = new SparkConf()
            .setAppName("KryoTest$")
            .setMaster("local[*]")
            .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
            .registerKryoClasses(Array(classOf[Searcher]))
        //创建SparkContext
        val sc: SparkContext = new SparkContext(conf)
    
        val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello atguigu", "atguigu", "hahah"), 2)
        
        val searcher = new Searcher("hello")
        val result: RDD[String] = searcher.getMatchedRDD1(rdd)
        
        result.collect.foreach(println)
        
        //关闭SparkContext
        sc.stop()
    }
}

case class Searcher(val query: String) {
    def isMatch(s: String): Boolean = {
        s.contains(query)
    }
    
    def getMatchedRDD1(rdd: RDD[String]): RDD[String] = {
        rdd.filter(isMatch)
        //rdd.filter(this.isMatch)
    }
    
    def getMatchedRDD2(rdd: RDD[String]): RDD[String] = {
        val q: String = query
        rdd.filter(_.contains(q))
        //rdd.filter(_.contains(this.query))
    }
}
