package com.atguigu.gmall.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.common.constant.GmallConstant
import com.atguigu.gmall.realtime.bean.Startuplog
import com.atguigu.gmall.realtime.util.{DateUtil, MyKafkaUtil, RedisUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object DauApp {

    def main(args: Array[String]): Unit = {
        //创建上下文环境对象
        val sparkConf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")
        val ssc = new StreamingContext(sparkConf,Seconds(5))

        //从kafka消费数据
        val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP,ssc)

     //   kafkaDStream.foreachRDD(rdd=> println(rdd.map(_.value()).collect().mkString("\n")))

        //将数据转换成Startuplog,补充两个时间段
        val startUpDStream: DStream[Startuplog] = kafkaDStream.map(rdd => {
            val jsonString: String = rdd.value()
            val startuplog: Startuplog = JSON.parseObject(jsonString, classOf[Startuplog])
            val stringTime: String = DateUtil.formatStringByTimestamp(startuplog.ts, "yyyy-MM-dd HH")

            val times: Array[String] = stringTime.split(" ")
            startuplog.logDate = times(0)
            startuplog.logHour = times(1)
            startuplog
        })


       // startUpDStream.foreachRDD(rdd=>rdd.foreach(println))


        //数据去重
        //过滤重复数据
        val filterDStream: DStream[Startuplog] = startUpDStream.transform(rdd => {
            println("过滤前："+rdd.count())
            val client: Jedis = RedisUtil.getJedisClient
            val key: String = "dau:" + DateUtil.formatStringByTimestamp(System.currentTimeMillis(), "yyyy-MM-dd")
            val midSet: util.Set[String] = client.smembers(key)
            client.close()
            val midBroadcast: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(midSet)

            val filterRDD: RDD[Startuplog] = rdd.filter(startUpLog => {
                !midBroadcast.value.contains(startUpLog.mid)
            })
            println("过滤后："+filterRDD.count())
            filterRDD
        })

        val distinctDStream: DStream[Startuplog] = filterDStream.map(startuplog => (startuplog.mid, startuplog)).groupByKey().flatMap {
            case (mid, startuplogiter) => {
                startuplogiter.toList.sortBy(_.ts).take(1)
            }
        }



        //保存所有今天访问过的用户到redis
        distinctDStream.foreachRDD(rdd=>{
            rdd.foreachPartition{
                datas=>
                    val jedisClient: Jedis = RedisUtil.getJedisClient
                    datas.foreach{
                    data=>{
                    val key = "dau:"+data.logDate
                    jedisClient.sadd(key,data.mid)
                    }
                        jedisClient.close()
                    }
            }
        })

        import org.apache.phoenix.spark._

        //将数据存入hbase
        distinctDStream.foreachRDD(rdd=>{
            rdd.saveToPhoenix("gmall_dau",Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),new Configuration,Some("hadoop102,hadoop103,hadoop104:2181"))
        })





        ssc.start()
        ssc.awaitTermination()
    }

}
