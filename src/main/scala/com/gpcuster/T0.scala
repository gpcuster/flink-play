package com.gpcuster

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object T0 {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.createLocalEnvironment()

    val text = env.socketTextStream("localhost", 9999)

    val counts = text.flatMap { _.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .sum(1)

    counts.print()

    env.execute("Window Stream WordCount")
  }
}
