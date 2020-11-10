package com.zack.apitest

import java.util

import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * com.zack.apitest
 *
 * @author zhangc
 * @version 1.0
 * @create 2020/11/10 14:12 
 */
object StateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //    val inputPath = "D:\\IdeaProjects\\flink-demo\\src\\main\\resources\\sensor.txt"
    //    val inputStream = env.readTextFile(inputPath)

    val inputStream = env.socketTextStream("192.168.1.52", 7777);

    val dataStream = inputStream
      .map( data => {
        val arr = data.split(",")
        SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
      } )

    //需求，对于温度传感器温度值跳变，超过10度，报警
    val alterStream = dataStream
        .keyBy(_.id)
        //.flatMap(new TempChangeAlter(10.0))
        .flatMapWithState[(String, Double, Double), Double]({
          case (data: SensorReading, None) => (List.empty, Some(data.temperature))
          case (data: SensorReading, lastTemp: Some[Double]) => {
            val diff = (data.temperature - lastTemp.get).abs
            if (diff > 10.0) {
              (List((data.id, lastTemp.get, data.temperature)), Some(data.temperature))
            }
            else {
              (List.empty, Some(data.temperature))
            }
          }
        })

    alterStream.print()

    env.execute("state test")


  }

}

//实现自定义richFlatmapFunction
class TempChangeAlter(d: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {

  //定义状态保存上一次温度值
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp" ,classOf[Double]))

  override def flatMap(value: SensorReading, collector: Collector[(String, Double, Double)]): Unit = {
    val lastTemp = lastTempState.value()
    //跟最新的温度值做差值
    val diff = (value.temperature - lastTemp).abs
    if (diff > d) {
      collector.collect(value.id, lastTemp, value.temperature)
    }

    //更新状态
    lastTempState.update(value.temperature)
  }
}


// key state 测试：必须定义在RichFunction中，因为需要运行时上下文
class MyRichMapper extends RichMapFunction[SensorReading, String]{

  var valueState: ValueState[Double] = _
  lazy val listState: ListState[Int] = getRuntimeContext.getListState(new ListStateDescriptor[Int]("liststate", classOf[Int]))
  override def open(parameters: Configuration): Unit = {
    valueState = getRuntimeContext.getState(new ValueStateDescriptor[Double] ("valuestate", classOf[Double]))
  }

  override def map(in: SensorReading): String = {

    //状态的读写
    val myV = valueState.value()
    valueState.update(in.temperature)
    listState.add(1)
    val list = new util.ArrayList[Int]()
    list.add(2)
    list.add(3)
    listState.addAll(list)
    listState.update(list)

    in.id
  }
}
