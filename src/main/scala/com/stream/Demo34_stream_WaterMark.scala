package com.stream

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.{ReducingState, ReducingStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers._
import org.apache.flink.streaming.api.windowing.windows.TimeWindow


object Demo34_stream_WaterMark{
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

   env.socketTextStream("mini1",6666)
      .flatMap(_.split("\\s+"))
      .map((_,1))
      .keyBy(0)
      .timeWindow(Time.seconds(20))
      .trigger(new MyTrigger)
      .sum(1)
      .print("tigger--------->>>>")

  env.execute()
  }
 }
