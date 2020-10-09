package com.atguigu.sparksql.day01

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSql08_Hive {
    def main(args: Array[String]): Unit = {
        // 创建SparkConf
        val conf: SparkConf = new SparkConf().setAppName("SparkSql08_Hive$").setMaster("local[*]")
        // 创建SparkSession
        val spark: SparkSession = SparkSession
            .builder()
            .config(conf)
            .enableHiveSupport()
            .getOrCreate()
        //导入隐式转换
        
        //建表
        spark.sql("show tables").show()
        spark.sql("select * from emp").show()
        
        // 关闭SparkSession
        spark.stop()
    }
}
