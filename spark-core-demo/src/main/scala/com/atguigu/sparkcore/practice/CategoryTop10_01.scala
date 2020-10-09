package com.atguigu.sparkcore.practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 分别统计每个品类点击的次数，下单的次数和支付的次数：
 * （品类，点击总数）（品类，下单总数）（品类，支付总数）
 */
object CategoryTop10_01 {
    def main(args: Array[String]): Unit = {
        // 创建SparkConf
        val conf: SparkConf = new SparkConf().setAppName("CategoryTop10_01$").setMaster("local[*]")
        // 创建SparkContext
        val sc: SparkContext = new SparkContext(conf)
        
        val originalData: RDD[String] = sc.textFile("./input/user_visit_action.txt")
        originalData.cache()
        // 点击数
        val filterClickData: RDD[String] = originalData.filter ( line => {
                line.split("_")(6).toInt != -1
         })
        // line => (cate,1)
        val clickCate_1: RDD[(String, Int)] = filterClickData.map { line => {
                val cateId: String = line.split("_")(6)
                (cateId, 1)
            }
        }
        // (cate,1) (cate,1) => (cate,2)
        val reduceClickData: RDD[(String, Int)] = clickCate_1.reduceByKey(_ + _)
        //        reduceClickData.collect.foreach(println)
        
        // 下单数
        val filterOrderData: RDD[String] = originalData.filter(line => {
            line.split("_")(8) != "null"
        })
        val orderCate_1: RDD[(String, Int)] = filterOrderData.flatMap(line => {
            line.split("_")(8).split(",")
        }).map((_, 1))
        val reduceOrderData: RDD[(String, Int)] = orderCate_1.reduceByKey(_ + _)
        
        // 支付数
        val filterPayData: RDD[String] = originalData.filter(line => {
            line.split("_")(10) != "null"
        })
        val payCate_1: RDD[(String, Int)] = filterPayData.flatMap(line => {
            line.split("_")(10).split(",")
        }).map((_, 1))
        val reducePayData: RDD[(String, Int)] = payCate_1.reduceByKey(_ + _)
        
        //Join三组数据
        val joinData: RDD[(String, ((Int, Option[Int]), Option[Int]))] = reduceClickData.leftOuterJoin(reduceOrderData)
            .leftOuterJoin(reducePayData)
        val result: RDD[(String, (Int, Int, Int))] = joinData.map { case (cateId, ((click, order), pay)) =>
            (cateId, (click, order.getOrElse(0), pay.getOrElse(0)))
        }
        result.sortBy(_._2,false).take(10).foreach(println)
        
        // 关闭SparkContext
        sc.stop()
    }
}
