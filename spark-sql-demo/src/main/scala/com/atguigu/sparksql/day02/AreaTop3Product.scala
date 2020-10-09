package com.atguigu.sparksql.day02

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable

object AreaTop3Product {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("AreaTop3Product").setMaster("local[*]")

    //2.创建SparkSession
    val spark: SparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    //注册UDAF函数
    spark.udf.register("cityRatio", functions.udaf(new AreaTop3ProductUDAF))

    //3.读取并过滤Hive数据
    spark.sql(
      """
        |select
        |    ci.area,
        |    pi.product_name,
        |    ci.city_name
        |from (select * from user_visit_action where click_product_id != '-1') uv
        |join product_info pi
        |on uv.click_product_id = pi.product_id
        |join city_info ci
        |on uv.city_id = ci.city_id
      """.stripMargin).createTempView("tmpAreaProduct")

    //4.计算各个大区中对于某个商品的点击总次数
    spark.sql(
      """
        |select
        |    area,
        |    product_name,
        |    count(*) ct,
        |    cityRatio(city_name) cityratio
        |from
        |    tmpAreaProduct
        |group by area,product_name
      """.stripMargin).createTempView("tmpAreaProductCount")

    //5.计算各个大区中商店点击次数排行
    spark.sql(
      """
        |select
        |    area,
        |    product_name,
        |    ct,
        |    cityratio,
        |    rank() over(partition by area order by ct desc) rk
        |from
        |    tmpAreaProductCount
      """.stripMargin).createTempView("tmpAreaProductCountRank")

    //6.取各个大区中商品点击次数前三名的数据
    spark.sql(
      """
        |select
        |   area,
        |   product_name,
        |   ct,
        |   cityratio
        |from
        |   tmpAreaProductCountRank
        |where rk <= 3
      """.stripMargin).show(100, truncate = false)

    //7.关闭资源
    spark.stop()

  }

}


class AreaTop3ProductUDAF extends Aggregator[String, mutable.HashMap[String, Int], String] {

  //初始化
  override def zero: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()

  //区内聚合
  override def reduce(b: mutable.HashMap[String, Int], a: String): mutable.HashMap[String, Int] = {
    b(a) = b.getOrElse(a, 0) + 1
    b
  }

  //区间聚合
  override def merge(b1: mutable.HashMap[String, Int], b2: mutable.HashMap[String, Int]): mutable.HashMap[String, Int] = {
    b2.foreach { case (cityName, count) =>
      b1(cityName) = b1.getOrElse(cityName, 0) + count
    }
    b1
  }

  //最终计算
  override def finish(reduction: mutable.HashMap[String, Int]): String = {

    //a.获取当前大区当前商品点击总数
    val totalCount: Int = reduction.values.sum

    //b.获取点击次数为前两名的城市
    val top2CityCount: List[(String, Int)] = reduction.toList.sortWith(_._2 > _._2).take(2)

    //c.定义其他占比
    var otherRatio: Double = 100D

    //d.取前两名城市的占比
    val ratios: List[CityRatio1] = top2CityCount.map { case (cityName, count) =>
      val ratio: Double = Math.round(count * 1000D / totalCount) / 10D
      otherRatio -= ratio
      CityRatio1(cityName, ratio)
    }

    //e.将其他占比放入集合
    val result: List[CityRatio1] = ratios :+ CityRatio1("其他", Math.round(otherRatio * 10D) / 10D)

    //f.返回值
    result.mkString(",")
  }

  override def bufferEncoder: Encoder[mutable.HashMap[String, Int]] = Encoders.kryo(classOf[mutable.HashMap[String, Int]])

  override def outputEncoder: Encoder[String] = Encoders.STRING
}

case class CityRatio1(cityName: String, ratio: Double) {
  override def toString: String = {
    s"$cityName$ratio%"
  }
}