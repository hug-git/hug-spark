package com.atguigu.sparksql.day02

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession, functions}

import scala.collection.mutable

object AreaTop3 {
    def main(args: Array[String]): Unit = {
        // 创建SparkConf
        val conf: SparkConf = new SparkConf().setAppName("AreaTop3$").setMaster("local[*]")
        // 创建SparkSession
        val spark: SparkSession = SparkSession.builder()
            .config(conf)
            .enableHiveSupport()
            .getOrCreate()
        //导入隐式转换
        import spark.implicits._
        
        val start: Long = System.currentTimeMillis()
        //过滤
        spark.sql(
            """
              |select
              |  c.area,
              |  p.product_name,
              |  c.city_name
              |from
              |  (select * from user_visit_action where click_product_id != "-1") u
              |join product_info p
              |on u.click_product_id=p.product_id
              |join city_info c
              |on u.city_id=c.city_id
              |""".stripMargin).createTempView("area_pro_city")
        spark.udf.register("cityRatio", functions.udaf(new CityRatio))
        
        //计算概率
        spark.sql(
            """
              |select
              |  area,
              |  product_name,
              |  count(1) ct,
              |  cityRatio(city_name) city_ratio
              |from
              |  area_pro_city
              |group by area,product_name
              |""".stripMargin).createTempView("countCityRatio")
        //分区排序
        spark.sql(
            """
              |select
              |  area,
              |  product_name,
              |  ct,
              |  city_ratio,
              |  rank() over(partition by area order by ct desc) rk
              |from
              |  countCityRatio
              |""".stripMargin).createTempView("countRank")
        //Top3
        spark.sql(
            """
              |select
              |  area,
              |  product_name,
              |  ct,
              |  city_ratio
              |from
              |  countRank
              |where rk <= 3
              |""".stripMargin).show(30, truncate = false)
        
        // 关闭SparkSession
        spark.stop()
        val end: Long = System.currentTimeMillis()
        println(end - start)
    }
}

class CityRatio extends Aggregator[String, mutable.HashMap[String, Int], String] {
    override def zero: mutable.HashMap[String, Int] = mutable.HashMap[String,Int]()
    
    override def reduce(b: mutable.HashMap[String, Int], a: String): mutable.HashMap[String, Int] = {
        b(a) = b.getOrElse(a, 0) + 1
        b
    }
    
    override def merge(b1: mutable.HashMap[String, Int], b2: mutable.HashMap[String, Int]): mutable.HashMap[String, Int] = {
        b2.foreach(kv => b1(kv._1) = b1.getOrElse(kv._1, 0) + kv._2)
        b1
    }
    
    override def finish(reduction: mutable.HashMap[String, Int]): String = {
        val totalCount: Int = reduction.values.sum
        
        val tu2pCityCount: List[(String, Int)] = reduction.toList.sortWith(_._2 > _._2).take(2)
        
        var otherRatio = 100D
        
        //(beijing,21)(shanghai,17)...
        val list: List[(String, Double)] = tu2pCityCount.map(
            area_count => {
                val radio: Double = Math.round(area_count._2 * 1000D / totalCount) / 10D
                otherRatio -= radio
                (area_count._1, radio)
            }
         )
        val newList: List[(String, Double)] = list :+ ("其他", Math.round(otherRatio * 10D) / 10D)
        val result: List[String] = newList.map(x => s"${x._1}${x._2}%")
        result.mkString(",")
    }
    
    override def bufferEncoder: Encoder[mutable.HashMap[String, Int]] = Encoders.kryo(classOf[mutable.HashMap[String, Int]])
    
    override def outputEncoder: Encoder[String] = Encoders.STRING
}
