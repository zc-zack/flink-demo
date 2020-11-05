package com.zack.apitest

import org.apache.flink.api.common.functions.{FilterFunction, ReduceFunction, RichMapFunction}
import org.apache.flink.streaming.api.scala._

import Array._

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
      //.filter(new MyFilter)

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
    dataStream.keyBy("id").reduce(new MyReduceFunction)



    //多流转换操作
    //将传感亲分为低温和高温
    val splitStream = dataStream
      .split(data => {
        if(data.temperature > 30.0) Seq("high") else Seq("low")
      })
    val highTempStream = splitStream.select("high");
    val lowTempStream = splitStream.select("low")
    val allTempStream = splitStream.select("high", "low")

//    highTempStream.print("high")
//    lowTempStream.print("low")
//    allTempStream.print("all")
    val warningStream = highTempStream.map(data => (data.id, data.temperature))
    val connectedStreams = warningStream.connect(lowTempStream)

    //用coMap对数据进行分别处理
    val coMapResultStream = connectedStreams
        .map(
          warningData => (warningData._1, warningData._2, "waring"),
          lowTempData => (lowTempData.id, "healthy")
        )

    //coMapResultStream.print()

    val unionStream = highTempStream.union(lowTempStream, allTempStream)


    env.execute("Transform test")
  }
}

class MyReduceFunction extends ReduceFunction[SensorReading] {
  override def reduce(t: SensorReading, t1: SensorReading): SensorReading =
    SensorReading(t.id, t1.timestamp, t.temperature.min(t1.temperature))
}

// 自定义函数类
class MyFilter extends FilterFunction[SensorReading] {
  override def filter(t: SensorReading): Boolean =
    t.id.startsWith("sensor_1")
}

//富函数，可以获得运行时上下文，还有一些生命周期
class MyRichMapper extends RichMapFunction[SensorReading, String] {
  override def map(in: SensorReading): String =
    in.id + "temperature"
}
