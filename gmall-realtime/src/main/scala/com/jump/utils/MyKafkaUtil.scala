package com.jump.utils

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

import java.util.Properties

/**
 * @ClassName MyKafkaUtil
 * @Description
 * @Author Four5prings
 * @Date 2022/6/30 19:20
 */
object MyKafkaUtil {
  private val pro: Properties = PropertiesUtil.load("config.properties")

  private val broker_list: String = pro.getProperty("kafka.broker.list")

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> broker_list,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean),
  )

    def getKafkaDStream(topic: String, ssc: StreamingContext) = {
      val stream = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](Array(topic), kafkaParams),
      )
      stream
    }
  /*
   def getKafkaStream(topic: String, ssc: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {

    val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam))

    dStream
  }
}*/

}
