package com.atguigu.sparkcore.practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SingleJumpRatio {
    def main(args: Array[String]): Unit = {
        // 创建SparkConf
        val conf: SparkConf = new SparkConf().setAppName("SingleJumpRatio$").setMaster("local[*]")
        // 创建SparkContext
        val sc: SparkContext = new SparkContext(conf)
        
        val oriData: RDD[String] = sc.textFile("./input/user_visit_action.txt")
        oriData.cache()
        
        //声明访问路径
        val pages = Array(1,2,3,4,5,6,7)
        
        //准备过滤条件
        //-单页过滤条件
        val fromPages: Array[Int] = pages.dropRight(1)
        //-单跳过滤条件
        val toPages: Array[Int] = pages.drop(1)
        val fromToPages: Array[String] = fromPages.zip(toPages).map{case (from,to) => s"${from}_$to"}
        
        //计算单页访问次数：分母
        val fromPageToCount: RDD[(String, Int)] = SingleJumpHandler.getSinglePageCount(oriData, fromPages)
        
        //计算单跳访问次数：分子
        val singleJumpPageCount: RDD[(String, Int)] = SingleJumpHandler.getSingleJumpCount(oriData,fromToPages)
        
        //转换singleJumpPageCount数据结构 (1_2,num) => (1,(1_2,num))
        val fromPageToSingleJumpPageCount: RDD[(String, (String, Int))] = singleJumpPageCount.map { case (fromToPage, count) =>
            (fromToPage.split("_")(0), (fromToPage, count))
        }
        val result: RDD[(String, Double)] = fromPageToSingleJumpPageCount.join(fromPageToCount)
            .map { case (fromPage, ((fromToPage, count1), count2)) =>
                (fromToPage, count1 / count2.toDouble)
            }
        result.sortByKey().collect.foreach(println)
        
        // 关闭SparkContext
        sc.stop()
    }
}
