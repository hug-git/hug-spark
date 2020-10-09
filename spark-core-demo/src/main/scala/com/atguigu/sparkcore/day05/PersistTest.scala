package com.atguigu.sparkcore.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD持久化
 */
object PersistTest {
    def main(args: Array[String]): Unit = {
        //创建SparkConf
        val conf: SparkConf = new SparkConf().setAppName("PersistTest$").setMaster("local[*]")
        //创建SparkContext
        val sc: SparkContext = new SparkContext(conf)
        
        //cacheTest(sc)
        checkpointTest(sc)
        
        //关闭SparkContext
        sc.stop()
    }
    
    /**
     * 持久化到内存中
     * @param sc SparkContext
     */
    def cacheTest(sc: SparkContext): Unit ={
        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5),2)
        val doubleRDD: RDD[Int] = rdd.map(x => {
            println(x + "-")
            x * 2
        })
        doubleRDD.cache()
    
        println(doubleRDD.collect().mkString(","))
        println("====================")
        doubleRDD.foreach(x => print("[" + x + "],"))
    }
    
    def checkpointTest(sc: SparkContext): Unit ={
        sc.setCheckpointDir("./ck")
        
        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5),2)
        val doubleRDD: RDD[Int] = rdd.map(x => {
            println(x + "-")
            x * 2
        })
        sc.setCheckpointDir("./ck")
        doubleRDD.checkpoint()
    
        println("====================")
        println(doubleRDD.collect().mkString(","))
        println("====================")
        doubleRDD.foreach(x => print("[" + x + "],"))
    }
}
