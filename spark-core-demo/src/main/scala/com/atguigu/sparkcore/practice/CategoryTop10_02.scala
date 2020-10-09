package com.atguigu.sparkcore.practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 一次性统计每个品类点击的次数，下单的次数和支付的次数：
 * （品类，（点击总数，下单总数，支付总数））
 */
object CategoryTop10_02 {
    def main(args: Array[String]): Unit = {
        // 创建SparkConf
        val conf: SparkConf = new SparkConf().setAppName("CategoryTop10_02$").setMaster("local[*]")
        // 创建SparkContext
        val sc: SparkContext = new SparkContext(conf)
    
        val originalData: RDD[String] = sc.textFile("./input/user_visit_action.txt")
        originalData.cache()
        
        val lineToTuple: RDD[(String, (Int, Int, Int))] = originalData.flatMap(line => {
            val arr: Array[String] = line.split("_")
            arr match {
                //点击数据
                case click if click(6).toInt != -1 => Array((click(6), (1, 0, 0)))
                //订单数据
                case order if order(8) != "null" => order(8).split(",").map((_, (0, 1, 0)))
                //支付数据
                case pay if pay(10) != "null" => pay(10).split(",").map((_, (0, 0, 1)))
                //其他
                case _ => Nil
            }
        })
        val result: RDD[(String, (Int, Int, Int))] = lineToTuple.reduceByKey((x,y)=> (x._1+y._1,x._2+y._2,x._3+y._3))
        
        result.sortBy(_._2, ascending = false).take(10).foreach(println)
        
        // 关闭SparkContext
        sc.stop()
    }
}
