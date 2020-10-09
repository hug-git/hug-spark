package com.atguigu.sparkcore.practice

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object CategoryTop10_03_1 {
    def main(args: Array[String]): Unit = {
        // 创建SparkConf
        val conf: SparkConf = new SparkConf().setAppName("CategoryTop10_03$").setMaster("local[*]")
        // 创建SparkContext
        val sc: SparkContext = new SparkContext(conf)
        
        //过滤掉“搜索”行为
        val originalData: RDD[String] = sc.textFile("./input/user_visit_action.txt")
        originalData.cache()
        
        //创建并注册累加器
        val accum: MyAccumulator2 = new MyAccumulator2
        sc.register(accum)
        
        test(originalData, accum)
        
        // 关闭SparkContext
        sc.stop()
        
    }
    
    
    def test(originalData: RDD[String], accum: MyAccumulator2): Unit = {
        originalData.foreach(line => {
            val arr: Array[String] = line.split("_")
            arr match {
                case click if click(6).toInt != -1 => accum.add(s"click_${click(6)}")
                case order if order(8) != "null" => order(8).split(",").foreach(x => accum.add(s"order_$x"))
                case pay if pay(10) != "null" => pay(10).split(",").foreach(x => accum.add(s"pay_$x"))
                case _ => Nil
            }
        })
        //=>(1,(click_1,num))
        val groupVal: Map[String, mutable.Map[String, Int]] = accum.value.groupBy(_._1.split("_")(1))
        val result: Map[String, (Int, Int, Int)] = groupVal.map {
            case (cate, map) => (cate, (map.getOrElse(s"click_$cate", 0), map.getOrElse(s"order_$cate", 0), map.getOrElse(s"pay_$cate", 0)))
        }
        val cateTop10: List[(String, (Int, Int, Int))] = result.toList.sortBy(_._2).reverse.take(10)
        cateTop10.foreach(println)
        
        println("==========================")
        val cateList: List[String] = cateTop10.map(_._1)
        val filterData: RDD[String] = originalData.filter(id => cateList.contains(id.split("_")(6)))
        //((category,session),1)
        val cate_session_1: RDD[((String, String), Int)] = filterData.map(line => {
            val arr: Array[String] = line.split("_")
            ((arr(6), arr(2)), 1)
        })
        // =>((category,session),n)
        val cate_sessionCount: RDD[((String, String), Int)] = cate_session_1.reduceByKey(_+_)
        // =>(category,(session,n))
        val cate_iter: RDD[(String, Iterable[(String, Int)])] = cate_sessionCount.map {
            case ((category, session), num) => (category, (session, num))
        }.groupByKey()
        val result1: RDD[(String, (String, Int))] = cate_iter.mapValues(iter => iter.toList.sortWith(_._2 >_._2).take(10))
            .flatMapValues(list => list)
        result1.foreach(println)
    }
}

case class MyAccumulator2() extends AccumulatorV2[String, mutable.Map[String, Int]] {
    private val map: mutable.Map[String, Int] = mutable.Map[String, Int]()
    
    override def isZero: Boolean = map.isEmpty
    
    override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = new MyAccumulator2
    
    override def reset(): Unit = map.clear()
    
    override def add(v: String): Unit = {
        map(v) = map.getOrElse(v, 0) + 1
    }
    
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
        other.value.foreach(
                kv => map(kv._1) = map.getOrElse(kv._1, 0) + kv._2
            )
    }
    
    override def value: mutable.Map[String, Int] = map
}
