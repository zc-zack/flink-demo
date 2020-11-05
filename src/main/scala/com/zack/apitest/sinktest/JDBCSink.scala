package com.zack.apitest.sinktest

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.zack.apitest.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._

/**
 * com.zack.apitest.sinktest
 *
 * @author zhangc
 * @version 1.0
 * @create 2020/11/5 15:37 
 */
object JDBCSink {
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


    dataStream.addSink(new MyJdbcSinkFunc)

    env.execute("JDBC sink test")
  }
}

class MyJdbcSinkFunc extends RichSinkFunction[SensorReading] {

  //定义连接 预编译语句

  var connection: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "root");
    insertStmt = connection.prepareStatement("insert into sensor (id, temp) values (?, ?)")
    updateStmt = connection.prepareStatement("update sensor set temp = ? where id = ?")
  }

  override def invoke(value: SensorReading): Unit = {
    //先执行更新操作，查到就更新
    updateStmt.setDouble(1, value.temperature)
    updateStmt.setString(2, value.id)
    updateStmt.execute()

    //如果没有更新数据，就插入数据
    if (updateStmt.getUpdateCount == 0) {
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(2, value.temperature)
      insertStmt.execute()
    }
  }

  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    connection.close()
  }
}

