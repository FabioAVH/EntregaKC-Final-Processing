//  Entrega Proyecto Final
//  Fabio Vargas
//  Batch Layer

package io.keepcoding.spark.exercise.batch

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.OffsetDateTime


object BatchJobImplFV extends BatchJob {

  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  override def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame = {
    spark
      .read
      .format("parquet")
      .load("/temporalFVFinal/data-spark")
      .where($"year" === lit(filterDate.getYear) &&
        $"month" === lit(filterDate.getMonthValue) &&
        $"day" === lit(filterDate.getDayOfMonth) &&
        $"hour" === lit(filterDate.getHour))
  }

  override def readAntennaMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = ???

  override def readUserMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .load()
  }

  override def enrichAntennaWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame = {
    antennaDF.as("a")
      .join(metadataDF.as("b"), $"a.id" === $"b.id")
      .drop($"b.id")
  }

  override def computeDevicesCountByCoordinates(dataFrame: DataFrame): DataFrame = ???
  override def computeErrorAntennaByModelAndVersion(dataFrame: DataFrame): DataFrame = ???
  override def computePercentStatusByID(dataFrame: DataFrame): DataFrame = ???

  // 1. Total de bytes recibidos por antena
  override def computeTotalBytesRxAntenna(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp",$"antenna_id",$"bytes")
      .groupBy($"antenna_id", window($"timestamp","1 hour"))
      .agg(sum($"bytes").as("SumBytes_RXAnt"))
      .select($"antenna_id", $"SumBytes_RxAnt")
  }
  // 2. Total de bytes transmitidos por correo de usuario
  override def computeTotalBytesTxEmail(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp",$"id",$"bytes",$"email")
      .groupBy($"id", $"email" ,window($"timestamp","1 hour"))
      .agg(sum($"bytes").as("SumBytes_TxEmail"))
      .select($"email", $"SumBytes_TxEmail")
  }
  //  3. Total de bytes transmitidos por aplicación
  override def computeTotalBytesTxApp(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp",$"app",$"bytes")
      .groupBy($"app", window($"timestamp","1 hour"))
      .agg(sum($"bytes").as("SumBytes_TxApp"))
      .select($"app", $"SumBytes_TxApp")
  }

  override def EmailOverQuota(dataFrame: DataFrame): DataFrame = {

    dataFrame
      .select($"timestamp",$"bytes",$"email",$"quota")
      .groupBy($"email", $"quota", window($"timestamp","1 hour"))
      .agg(sum($"bytes").as("consumo"))
      .where($"consumo" > lit($"quota"))
      .select($"email",$"quota",$"consumo")
  }


  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit = {
    dataFrame
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .save()
  }

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Unit = ???


  //------------------------------------ MAIN ------------------------------------//

  def main(args: Array[String]): Unit = {

    val rawDF = readFromStorage("/temporalFVFinal/data-spark", OffsetDateTime.parse("2022-02-27T13:00:00Z"))

    val metadataUserDF = readUserMetadata("jdbc:postgresql://34.71.108.180:5432/postgres",
      "user_metadata",
      "postgres",
      "1234")

   val enrichDF = enrichAntennaWithMetadata(rawDF, metadataUserDF)

    enrichDF.show()

    //Pregunta numero 1
    writeToJdbc(computeTotalBytesRxAntenna(enrichDF),
      "jdbc:postgresql://34.71.108.180:5432/postgres",
      "tableexe1batch",
      "postgres",
      "1234")

    //Pregunta numero 2
    writeToJdbc(computeTotalBytesTxEmail(enrichDF),
      "jdbc:postgresql://34.71.108.180:5432/postgres",
      "tableexe2batch",
      "postgres",
      "1234")

    //Pregunta numero 3
    writeToJdbc(computeTotalBytesTxApp(enrichDF),
      "jdbc:postgresql://34.71.108.180:5432/postgres",
      "tableexe3batch",
      "postgres",
      "1234")

    //Pregunta numero 4
    writeToJdbc(EmailOverQuota(enrichDF),
      "jdbc:postgresql://34.71.108.180:5432/postgres",
      "tableexe4batch",
      "postgres",
      "1234")

  } //Final Main

}


/*
    enrichDF.show()
    computeTotalBytesRxAntenna(enrichDF)
      .show()
     computeTotalBytesTxEmail(enrichDF)
      .show()
    computeTotalBytesTxApp(enrichDF)
      .show()
    EmailOverQuota(enrichDF)
      .show()

     */
