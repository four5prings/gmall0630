package com.jump.app

import com.alibaba.fastjson.JSON
import com.jump.bean.OrderInfo
import com.jump.constants.GmallConstants
import com.jump.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

/**
 * @ClassName GmvApp
 * @Description
 * @Author Four5prings
 * @Date 2022/6/30 22:05
 */
object GmvApp {
  def main(args: Array[String]): Unit = {
    // 创建StreamingContext并获取kafka流
    val conf: SparkConf = new SparkConf().setAppName("GmvApp").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(3))
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaDStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)

    // 转换数据格式
    val orderInfoDStream: DStream[OrderInfo] = kafkaDStream.map(record => {
      //使用json解析器转换格式
      val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
      orderInfo.create_date = orderInfo.create_time.split(" ")(0)
      orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)
      orderInfo
    })

    //将数据存入HBase
    orderInfoDStream.foreachRDD(rdd =>{
      rdd.saveToPhoenix(
        "GMALL2022_ORDER_INFO",
        Seq("ID","PROVINCE_ID","CONSIGNEE","ORDER_COMMENT","VARCONSIGNEE_TEL",
          "ORDER_STATUS","PAYMENT_WAY","USER_ID","IMG_URL","TOTAL_AMOUNT",
          "EXPIRE_TIME","DELIVERY_ADDRESS","CREATE_TIME","OPERATE_TIME",
          "TRACKING_NO","PARENT_ORDER_ID","OUT_TRADE_NO","TRADE_BODY",
          "CREATE_DATE","CREATE_HOUR"),
        HBaseConfiguration.create(),
        zkUrl = Some("hadoop102,hadoop103,hadoop104:2181")
      )
    })

    // 启动并阻塞
    ssc.start()
    ssc.awaitTermination()
  }
}
