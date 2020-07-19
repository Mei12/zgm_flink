package com.stream

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.{ReducingState, ReducingStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.RichWindowFunction
import org.apache.flink.streaming.api.{TimeCharacteristic, watermark}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers._
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


object Demo34_stream_WaterMark{
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

   env.socketTextStream("mini1",6666)
      .filter(_.nonEmpty)
       .map(x=>{
         val strings: Array[String] = x.split("\\s+")
         (strings(0),strings(1).toLong)
       })
       .assignTimestampsAndWatermarks(new MyATAW )
       .keyBy(0)
       .timeWindow(Time.seconds(3))
       .apply(new RichWindowFunction[(String,Long),String,Tuple,TimeWindow] {
         override def apply(key: Tuple, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[String]): Unit = {
           val lst: List[(String, Long)] = input.iterator.toList.sortBy(_._2)
           val start: Long = window.getStart
           val end: Long = window.getEnd
           val res=s"**窗口触发**" +
           s"事件条数-》${lst.size}" +
             s"事件的最小时间-》${lst.head._2}" +
             s"事件的最大时间-》${lst.last._2}" +
           s"窗口开始时间-》${start}" +
             s"窗口结束时间-》${end}"
           out.collect(res)
         }
       })
       .print()

  env.execute()
  }
 }
class MyATAW extends  AssignerWithPeriodicWatermarks[(String,Long)] {
  var max_Timestamp=0L
  val latenss=10*1000

  override def getCurrentWatermark: Watermark = {
    new Watermark(max_Timestamp-latenss)
  }

  override def extractTimestamp(t: (String, Long), l: Long): Long = {
    val now_timestamp=t._2
   max_Timestamp= Math.max(now_timestamp,max_Timestamp)

    println(s"nowtime:${now_timestamp}--------max_timestamp:${max_Timestamp}---------waterMark:${getCurrentWatermark.getTimestamp}")

    now_timestamp
  }
}