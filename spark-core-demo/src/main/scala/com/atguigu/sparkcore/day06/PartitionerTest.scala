package com.atguigu.sparkcore.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object PartitionerTest {
    def main(args: Array[String]): Unit = {
        //创建SparkConf
        val conf: SparkConf = new SparkConf().setAppName("PartitionerTest$").setMaster("local[*]")
        //创建SparkContext
        val sc: SparkContext = new SparkContext(conf)
        
        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,7),2)
        val value_1: RDD[(Int, Int)] = rdd.map((_,1))
    
        //进行自定义分区
        val valuePar: RDD[(Int, Int)] = value_1.partitionBy(new Partitioner {
            override def numPartitions: Int = 3
        
            override def getPartition(key: Any): Int = {
                key match {
                    case x:Int if x < 5 => 0
                    case 6 => 1
                    case _ => 2
                }
            }
        })
        valuePar.mapPartitionsWithIndex((index,iter) => iter.map((index,_))).collect.foreach(println)
        
        //关闭SparkContext
        sc.stop()
    }
}

class MyPartitioner(parNum: Int) extends Partitioner{
    override def numPartitions: Int = parNum
    
    override def getPartition(key: Any): Int = {
        0
    }
}
