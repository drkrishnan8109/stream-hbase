package com.streamer

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.mapred.TableOutputFormat

object HbasePersister {

  import org.apache.hadoop.conf.Configuration

  def main(args: Array[String]) {
    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("StreamHbasePersister")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "earliest", //testing with earliest, has to set to latest
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("iottopic")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val conf = HBaseConfiguration.create();
    val tableName = "iotinfo"

    stream.foreachRDD { rdd =>
      rdd.foreachPartition(x => {
        val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
        hBaseWriter(x, spark, tableName, conf)
      })
    }

    stream.foreachRDD { rdd =>
      rdd.foreachPartition(x => {
        val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
        hBaseWriter(x, spark, tableName, conf)
      })
    }

    ssc.start()
    try {
      ssc.awaitTermination()
    } catch {
      case e: Exception =>
        ssc.stop(stopSparkContext = true, stopGracefully = true)
        throw e // to exit with error condition
    }
  }

  def hBaseWriter(
      records: Iterator[ConsumerRecord[String, String]],
      spark: SparkSession,
      tableName: String,
      conf: Configuration
  ): Unit = {

    val conf = HBaseConfiguration.create()
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    import org.apache.hadoop.hbase.client.ConnectionFactory
    val connection = ConnectionFactory.createConnection(conf)
    val table = connection.getTable(TableName.valueOf(tableName))

    records.foreach(rec => {
      import com.model.IotInfo
      import spark.implicits._

      /*val s = new com.google.gson.Gson().fromJson(rec.value(), classOf[IotInfo])
      val ds = Seq(s).toDS()*/

      val ds = Seq(rec.value()).toDF().as[IotInfo]

      ds.foreach(event => {
        import com.model.Data
        import org.apache.hadoop.hbase.util.Bytes
        val (imm, p) = Data.convertToPut(event.data)

        p.addColumn(
          Bytes.toBytes("data"),
          Bytes.toBytes("entity"),
          Bytes.toBytes(event.hashCode())
        )
        table.put(p)
      })
      // puts.saveAsNewAPIHadoopDataset(jobConfig)
    })
  }

  def hbaseBulkWriter(
      records: Iterator[ConsumerRecord[String, String]],
      spark: SparkSession,
      tableName: String,
      conf: HBaseConfiguration
  ): Unit = {}
}
