package com.zack.apitest

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._

/**
 * com.zack.apitest
 *
 * @author zhangc
 * @version 1.0
 * @create 2020/11/3 14:33 
 */
object TransformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val inputSteam = env.readTextFile("D:\\IdeaProjects\\flink-demo\\src\\main\\resources\\sensor.txt");
    env.setParallelism(1)
    //样例转换
    val dataStream = inputSteam.map(data => {
      val arr = data.split(",")
      SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
    })

    // 分组聚合，输出每个传感器最小值
    val aggStream = dataStream
      .keyBy("id")//根据id进行分组
      .minBy("temperature")

    //需要输出当前最小的温度值以及最近的时间戳
    val resultStream = dataStream
        .keyBy("id")
        .reduce((currentState, newData) =>
          SensorReading(currentState.id, newData.timestamp, currentState.temperature.min(newData.temperature))
        )

    aggStream.print()
    env.execute("Transform test")
  }
}

class MyReduceFunction extends ReduceFunction[SensorReading] {
  override def reduce(t: SensorReading, t1: SensorReading): SensorReading =
    SensorReading(t.id, t1.timestamp, t.temperature.min(t1.temperature))
}
