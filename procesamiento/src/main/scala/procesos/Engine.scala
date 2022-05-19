package procesos

import com.bbva.datioamproduct.fdevdatio.utils.IOUtils
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, desc}
import org.slf4j.{Logger, LoggerFactory}
import procesos.common.ConfigConstants

import scala.util.{Failure, Success, Try}

class Engine extends SparkProcess with IOUtils {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  //RuntimeContext : Contiene configuraciones para la ejecución.
  override def runProcess(runtimeContext: RuntimeContext): Int  = {

    Try {

      logger.info(s"Process Id : ${runtimeContext.getProcessId}") //Arroja el getProcessID : Engine

      val config: Config = runtimeContext.getConfig //Extracción del getConfig del runtimeContext (variables del proyecto).
      val devName: String = config.getString(ConfigConstants.DevName)

      // variables HOCON para el proyecto
      //procesamiento/src/test/resources/config/applicationLocal.conf , desarrollo en local se hace en test.
      logger.info(s"¿config es vacío? : ${config.isEmpty}" )
      logger.info(s"Contenido Dev Name en applicationLocal.conf : ${config.getString(ConfigConstants.DevName)}" )
      logger.info(s"Bienvenido a procesamiento de Datos $devName")

      val customerParquet: DataFrame = read(config.getConfig(ConfigConstants.CustomersParquet))
      val bikesDf: DataFrame = read(config.getConfig(ConfigConstants.BikesInput))
      val customerDf: DataFrame = read(config.getConfig(ConfigConstants.CustomersInput))

      println("Transformaciones de datos : ")

      bikesDf.printSchema()
      customerDf.printSchema()

      val filterBikes: DataFrame = bikesDf.filter(col("size") =!= "S")
      filterBikes.groupBy("size").count().show()

      val filterCustomers: DataFrame = customerDf.filter((col("purchase_city") =!= "Tokyo") && (col("purchase_year") > 2010))
      filterCustomers.groupBy("purchase_year","purchase_city")
        .count()
        .sort("purchase_year")
        .show()

    } match {
      case Failure(e) => -1
      case Success(_) => 0
    }
  }

  override def getProcessId: String = "Engine"

}
