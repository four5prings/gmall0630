package com.jump.app

import com.alibaba.fastjson.JSON
import com.jump.bean.StartUpLog
import com.jump.constants.GmallConstants
import com.jump.handler.DauHandler
import com.jump.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

import java.text.SimpleDateFormat

/**
 * @ClassName DauApp
 * @Description
 * @Author Four5prings
 * @Date 2022/6/30 19:28
 */
object DauApp {
  def main(args: Array[String]): Unit = {
    // create ssc
    val conf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    // get KafkaDStream,
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaDStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

    // 需求分析 日活。就是要对mid进行去重。我们这里实时采集明细数据到 Hbase中，
    /**
     * Hbase 吞吐量大  WAL预写日志保证数据安全性，内存中的 memStore写缓存保证了效率 rowKey的设计
     * 去重问题 就是对mid进行去重，批次间和批次内去重，批次间去重，那必然需要有一个数据库来缓存，这里选择redis
     * redis k-v 用什么来存储信息呢  去重幂等性，可以想到set。key呢 为了区别，dau+日期 value-set（mid）
     */

    //转换数据格式，因为kafkadstream里面的数据是json格式，不方便，而且我们还需要添加字段,使用日志里面的ts
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.map(rdd => {
      val startUpLog: StartUpLog = JSON.parseObject(rdd.value(), classOf[StartUpLog])
      val times: String = sdf.format(startUpLog.ts)
      startUpLog.logDate = times.split(" ")(0)
      startUpLog.logHour = times.split(" ")(1)
      startUpLog
    })
    //测试打印条数
    startUpLogDStream.count().print()
    // 批次间去重
    val filterByRedisDStream: DStream[StartUpLog] = DauHandler.filterByRedis(startUpLogDStream)

    //测试打印条数
    filterByRedisDStream.count().print()

    // 批次内去重
    val distinctDStream: DStream[StartUpLog] = DauHandler.filterByGroup(filterByRedisDStream)
    //测试打印条数
    distinctDStream.count().print()

    // 存入redis
    DauHandler.saveToRedis(distinctDStream)

    // 存入HBase、使用phoenix
    distinctDStream.foreachRDD(rdd =>{
      rdd.saveToPhoenix(
        "GMALL2022_DAU",
        Seq("MID","UID","APPID","AREA","OS","CH","TYPE","VS","LOGDATE","LOGHOUR","TS"),
        HBaseConfiguration.create(),
        zkUrl=Some("hadoop102,hadoop103,hadoop104:2181")
      )
    })

    // start and block
    ssc.start()
    ssc.awaitTermination()
  }
}
