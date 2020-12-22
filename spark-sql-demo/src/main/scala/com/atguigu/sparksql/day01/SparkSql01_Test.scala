package com.atguigu.sparksql.day01

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSql01_Test {
    def main(args: Array[String]): Unit = {
        // 创建SparkConf
        val conf: SparkConf = new SparkConf().setAppName("SparkSql01_Test$").setMaster("local[*]")
        // 创建SparkSession
        val ss: SparkSession = SparkSession.builder().config(conf).getOrCreate()
        //导入隐式转换
        import ss.implicits._
        
        val df: DataFrame = ss.read.json("./input/people.json")
        
        df.show()
        
        df.createOrReplaceTempView("people")
        
        ss.sql(
            """
              |select
              |    *
              |from
              |    people
              |""".stripMargin).show()
        
        //DSL风格
        df.select("name").show()
        
        //DF --> RDD
        val rdd: RDD[Row] = df.rdd
        rdd.collect.foreach(println)
        
        //RDD --> DF
        rdd.map(row => People(row.getString(1), row.getLong(0))).toDF().show()
        
        //DF --> DS
        val ds: Dataset[People] = df.as[People]
        ds.show()
        
        // DS --> DF
        val frame: DataFrame = ds.toDF()
    
        // RDD --> DS
        val ds1: Dataset[People] = rdd.map(row => People(row.getString(1), row.getLong(0))).toDS()
        
        // DS --> RDD
        val rdd1: RDD[People] = ds1.rdd
        
        // 关闭SparkSession
        ss.stop()
    }
}
case class People(name: String, age:Long)