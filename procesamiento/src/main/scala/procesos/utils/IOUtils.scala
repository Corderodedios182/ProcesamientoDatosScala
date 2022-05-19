package com.bbva.datioamproduct.fdevdatio.utils

import com.datio.dataproc.sdk.datiosparksession.DatioSparkSession
import com.datio.dataproc.sdk.schema.DatioSchema
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.net.URI

trait IOUtils {

  //Forma de decirle al lenguaje que prepare una variable para que este disponible cuando se necesite.
  //Es una lazy val por que necesito que este disponible en cualquier momento mi datioSparkSession.
  //si es solo una variable va arrojar un error de tipo nullpointerException.
  lazy val datioSparkSession: DatioSparkSession = DatioSparkSession.getOrCreate()

  def read(inputConfig: Config): DataFrame = {
    val path: String = inputConfig.getStringList("paths").get(0)

    inputConfig.getString("type") match {
      case "parquet" => datioSparkSession.read().parquet(path)

      case "csv" =>
        val schemaPath: String = inputConfig.getString("schema.path")
        val schema: DatioSchema = DatioSchema.getBuilder.fromURI(URI.create(schemaPath)).build()
        val delimiter: String = inputConfig.getString("delimiter")
        val header: String = inputConfig.getString("header")
        datioSparkSession.read()
          .option("delimiter", delimiter)
          .option("header", header)
          .datioSchema(schema)
          .csv(path)

      case _@inputType => throw new Exception(s"Formato de archivo no soportado: $inputType")
    }
  }
}
