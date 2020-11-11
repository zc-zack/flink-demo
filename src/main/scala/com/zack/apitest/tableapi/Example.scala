package com.zack.apitest.tableapi

import com.zack.apitest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala._


/**
 * com.zack.apitest.tableapi
 *
 * @author zhangc
 * @version 1.0
 * @create 2020/11/11 11:54 
 */
object Example {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputPath = "D:\\IdeaProjects\\flink-demo\\src\\main\\resources\\sensor.txt"
    val inputStream = env.readTextFile(inputPath)

    val dataStream = inputStream
      .map( data => {
        val arr = data.split(",")
        SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
      } )

    // 首先创建表执行环境
    val tableEnv = StreamTableEnvironment.create(env)

    //基于流创建一张表
     val dataTable: Table = tableEnv.fromDataStream(dataStream)

    //调用table api 进行转换
    val resultTable = dataTable
      .select("id, temperature")
      .filter("id == 'sensor_1'")

    // 直接用sql实现
    tableEnv.createTemporaryView("dataTable", dataTable)
    val sql: String = "select id, temperature from dataTable where id = 'sensor_1'"
    val resultSqlTable = tableEnv.sqlQuery(sql)

    resultTable.toAppendStream[(String, Double)].print("result")
    resultSqlTable.toAppendStream[(String, Double)].print("result sql")

    env.execute("table api example")
  }
}
