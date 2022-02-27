//  Entrega Proyecto Final
//  Fabio Vargas
//  Speed Layer

package io.keepcoding.spark.exercise.streaming

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object StreamJobImplFV extends StreamingJob {

  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  override def readFromKafka(kafkaServer: String, topic: String): DataFrame = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",kafkaServer)
      .option("subscribe",topic)
      .load()
  }

  override def parserJsonData(dataFrame: DataFrame): DataFrame = {

    val struct = StructType(Seq(
      StructField("timestamp",TimestampType, nullable = false),
      StructField("id",StringType, nullable = false),
      StructField("antenna_id",StringType, nullable = false),
      StructField("bytes",LongType, nullable = false),
      StructField("app",StringType, nullable = false)
    ))

    dataFrame
      .select(from_json($"value".cast(StringType), struct).as("value"))
      .select($"value.*")

  }

  override def readAntennaMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read
      .format("jdbc")
      .option("url",jdbcURI)
      .option("dbtable",jdbcTable)
      .option("user",user)
      .option("password",password)
      .load()
  }

  override def enrichAntennaWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame = {
    antennaDF.as("a")
      .join(metadataDF.as("b"),$"a.id" === $"b.id")
      .drop($"b.id")

  }

  // 1. Total de bytes recibidos por antena
  override def computeTotalBytesRxAntenna(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp",$"antenna_id",$"bytes")
      .withWatermark("timestamp","20 seconds")
      .groupBy($"antenna_id", window($"timestamp","90 seconds"))
      .agg(sum($"bytes").as("SumBytes_RXAnt"))
      .select($"antenna_id", $"SumBytes_RxAnt")
  }
  // 2. Total de bytes transmitidos por id de usuario
  override def computeTotalBytesTxUser(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp",$"id",$"bytes")
      .withWatermark("timestamp","15 seconds")
      .groupBy($"id", window($"timestamp","90 seconds"))
      .agg(sum($"bytes").as("SumBytes_TxUser"))
      .select($"id", $"SumBytes_TxUser")
  }
  //  3. Total de bytes transmitidos por aplicación
  override def computeTotalBytesTxApp(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp",$"app",$"bytes")
      .withWatermark("timestamp","15 seconds")
      .groupBy($"app", window($"timestamp","90 seconds"))
      .agg(sum($"bytes").as("SumBytes_TxApp"))
      .select($"app", $"SumBytes_TxApp")
  }

  override def computeDevicesCountByCoordinates(dataFrame: DataFrame): DataFrame = ???

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] = Future {
    dataFrame
      .writeStream
      .foreachBatch{(data:DataFrame, batchId: Long) =>
        data
          .write
          .mode(SaveMode.Append)
          .format("jdbc")
          .option("url",jdbcURI)
          .option("dbtable",jdbcTable)
          .option("user",user)
          .option("password",password)
          .save()
      }
      .start()
      .awaitTermination()
  }

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit] = Future {
    dataFrame
      .withColumn("year",year($"timestamp"))
      .withColumn("month",month($"timestamp"))
      .withColumn("day",dayofmonth($"timestamp"))
      .withColumn("hour",hour($"timestamp"))
      .writeStream
      .partitionBy("year","month","day","hour")
      .format("parquet")
      .option("path", storageRootPath)
      .option("checkpointLocation","/temporalFV/spark/checkpoint")
      .start()
      .awaitTermination()
  }


  //------------------------------------ MAIN ------------------------------------//

  def main(arg: Array[String]): Unit = {

  val DFParser = parserJsonData(
      readFromKafka("34.125.1.167:9092","devices"))


 //  val future1FV = writeToJdbc(computeTotalBytesRxAntenna(DFParser),"jdbc:postgresql://34.71.108.180:5432/postgres","brxantenna","postgres","1234")
 //  val future2FV = writeToJdbc(computeTotalBytesTxUser(DFParser),"jdbc:postgresql://34.71.108.180:5432/postgres","btxuser","postgres","1234")
 //  val future3FV = writeToJdbc(computeTotalBytesTxApp(DFParser),"jdbc:postgresql://34.71.108.180:5432/postgres","brxapp","postgres","1234")
  val future4FV = writeToStorage(DFParser,"/temporalFVFinal/data-spark")

  Await.result(Future.sequence(Seq(future4FV)), Duration.Inf)

 } // Final MAIN

}


/*
    //computeTotalBytesRxAntenna(DFParser)
    computeTotalBytesTxUser(DFParser)
      .writeStream
      .format("console")
      .start()
      .awaitTermination()
     */

