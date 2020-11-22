package com.gpcuster

import com.gpcuster.source.EventsGeneratorSource
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object T1 {
  def main(args: Array[String]): Unit = {
    val conf = new Configuration
    conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    val inputDS = env.addSource(new EventsGeneratorSource(1000))

    inputDS.print()

    env.execute("Test Source Function")
  }
}
