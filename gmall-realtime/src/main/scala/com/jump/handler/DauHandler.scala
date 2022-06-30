package com.jump.handler

import com.jump.bean.StartUpLog
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

import java.lang

/**
 * @ClassName DauHandler
 * @Description
 * @Author Four5prings
 * @Date 2022/6/30 19:53
 */
object DauHandler {
  /**
   * 批次内去重
   *
   * @param filterByRedisDStream
   * @return
   */
  def filterByGroup(filterByRedisDStream: DStream[StartUpLog]): DStream[StartUpLog] = {
    //批次内去重，使用groupByKey，然后按照时间戳排序，取最小的那一条日志
    val tupleDStream: DStream[((String, String), StartUpLog)] = filterByRedisDStream.map(log => {
      ((log.mid, log.logDate), log)
    })
    //聚合
    val result: DStream[StartUpLog] = tupleDStream.groupByKey()
      .mapValues(iter => {
        iter.toList.sortWith(_.ts < _.ts).take(1)
      }).flatMap(_._2)

    result
  }

  /**
   * 批次间去重
   *
   * @param startUpLogDStream
   * @return
   */
  def filterByRedis(startUpLogDStream: DStream[StartUpLog]): DStream[StartUpLog] = {
    //在分区内执行逻辑代码
    startUpLogDStream.mapPartitions(partition => {
      //减少连接创建数
      val jedis = new Jedis("hadoop102", 6379)
      //过滤出filter如果是ture就保留
      val logs: Iterator[StartUpLog] = partition.filter(log => {
        val redisKey: String = "Dau:" + log.logDate
        !jedis.sismember(redisKey, log.mid)
      })
      jedis.close()
      logs
    })
  }

  /**
   * 存入redis
   *
   * @param distinctDStream
   */
  def saveToRedis(distinctDStream: DStream[StartUpLog]) = {
    /*startUpLogDStream.mapPartitions(iter =>{
      val jedis = new Jedis("hadoop102", 6379)
      iter.foreach(log =>{

      })
      jedis.close()
    })*/
    //写库操作
    distinctDStream.foreachRDD(rdd => {
      //分区内 创建连接，减少jedis连接个数，同时是在executor端创建
      rdd.foreachPartition(iter => {
        //创建连接
        val jedis = new Jedis("hadoop102", 6379)
        //遍历迭代器，将数据写入redis中
        iter.foreach(log => {
          //这里的redisKey是Dau：+date。后续不能出错，否则无法去重
          val redisKey: String = "Dau:" + log.logDate
          jedis.sadd(redisKey, log.mid)
        })
        //关闭连接
        jedis.close()
      })
    })
  }

}
