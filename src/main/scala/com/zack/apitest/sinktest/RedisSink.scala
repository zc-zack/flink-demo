package com.zack.apitest.sinktest

import com.zack.apitest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.{FlinkJedisConfigBase, FlinkJedisPoolConfig}
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * com.zack.apitest.sinktest
 *
 * @author zhangc
 * @version 1.0
 * @create 2020/11/5 15:00 
 */
object RedisSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputPath = "D:\\IdeaProjects\\flink-demo\\src\\main\\resources\\sensor.txt"
    val inputStream = env.readTextFile(inputPath)

    val dataStream = inputStream.map(data => {
      val arr = data.split(",")
      SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
    })

    //定义一个flinkJedisConfigBase
    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost("localhost")
      .setPort(6379)
      .setPassword("123456")
      .build()



    dataStream.addSink(new RedisSink[SensorReading](conf, new MyRedisMapper))


    env.execute("redis sink test")
  }
}

class MyRedisMapper extends RedisMapper[SensorReading] {

  //定义保存数据写入redis命令 HSET 表名 key value
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET, "sensor_temp")
  }

  //将温度值指定为value
  override def getKeyFromData(t: SensorReading): String = t.id

  //将id指定为key
  override def getValueFromData(t: SensorReading): String = t.temperature.toString
}
