package com.stream

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector


/*
 * 控制操作链
 */
object Demo26_stream_Broadcast{
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境   --- scala包
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val d1: DataStream[(Int, Char)] = env.fromElements((1,'男'),(2,'女'),(3,'未'))

    val d2: DataStream[(String, String, Int,String)] = env.socketTextStream("mini1", 6666)
      .map(line => {
        val fields: Array[String] = line.split("\\s+")
        (fields(0), fields(1), fields(2).toInt,fields(3))

      })

    val genderInfo: MapStateDescriptor[Integer, Character] = new MapStateDescriptor(
      "genderInfo",
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.CHAR_TYPE_INFO
    )



    val broadcast: BroadcastStream[(Int, Char)] = d1.broadcast(genderInfo)


    d2.connect(broadcast)
        .process(new BroadcastProcessFunction[(String,String,Int,String),(Int,Char),(String,String,Char,String)] {
          override def processElement(in1: (String, String, Int, String),
                                      readOnlyContext: BroadcastProcessFunction[(String, String, Int, String), (Int, Char),
                                        (String, String, Char, String)]#ReadOnlyContext, collector: Collector[(String, String, Char, String)]): Unit = {
            var genderFlag=in1._3
            var gender: Character = readOnlyContext.getBroadcastState(genderInfo).get(genderFlag)
            if(gender==null){
            gender='o'
            }
            collector.collect((in1._1,in1._2,gender,in1._4))

          }

          override def processBroadcastElement(in2: (Int, Char),
                                               context: BroadcastProcessFunction[(String, String, Int, String), (Int, Char), (String, String, Char, String)]
                                                 #Context, collector: Collector[(String, String, Char, String)]): Unit ={
            context.getBroadcastState(genderInfo).put(in2._1,in2._2)
          }
        }).print("broadcast---")


    env.execute("-----broadcast test---")
  }
}
