package com.atguigu.sparksql.day01

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSql07_Mysql {
    def main(args: Array[String]): Unit = {
        // 创建SparkConf
        val conf: SparkConf = new SparkConf().setAppName("SparkSql07_Mysql$").setMaster("local[*]")
        // 创建SparkSession
        val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
        //导入隐式转换
        
        //读取MySQL数据创建DF
        val df: DataFrame = spark.read.format("jdbc")
            .option("url", "jdbc:mysql://hadoop102:3306/spark-sql")
            .option("driver", "com.mysql.jdbc.Driver")
            .option("user", "root")
            .option("password", "123456")
            .option("dbtable", "user")
            .load()
        
        //保存到MySQL
        df.write.format("jdbc")
            .option("url", "jdbc:mysql://hadoop102:3306/spark-sql")
            .option("driver", "com.mysql.jdbc.Driver")
            .option("user", "root")
            .option("password", "123456")
            .option("dbtable", "user01")
            .save()
        
        // 关闭SparkSession
        spark.stop()
    }
}
