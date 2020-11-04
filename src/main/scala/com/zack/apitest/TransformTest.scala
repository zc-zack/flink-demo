package com.zack.apitest

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream

/**
 * com.zack.apitest
 *
 * @author zhangc
 * @version 1.0
 * @create 2020/11/3 14:33 
 */
class TransformTest {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val streamFromFile = env.readTextFile("D:\\IdeaProjects\\flink-demo\\src\\main\\resources\\sensor.txt");



    env.execute("Transform test")
  }
}
