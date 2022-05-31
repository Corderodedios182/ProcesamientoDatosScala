package procesos

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}
import scala.util.{Failure, Success, Try}

import com.bbva.datioamproduct.fdevdatio.utils.IOUtils
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext

import procesos.common.ConfigConstants.{BikesInput, CurrentYear, CustomersInput, CustomersParquet, DevName}
import procesos.transormations.transformations.{BikesDf, CustomerDf, CustomersBikesDf}

class Engine extends SparkProcess with IOUtils {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  //RuntimeContext : Contiene configuraciones para la ejecución.
  override def runProcess(runtimeContext: RuntimeContext): Int  = {

    Try {

      logger.info(s"Process Id : ${runtimeContext.getProcessId}") //Arroja el getProcessID : Engine

      val config: Config = runtimeContext.getConfig //Extracción del getConfig del runtimeContext (variables del proyecto).
      val devName: String = config.getString(DevName)
      val currentYear: String = config.getString(CurrentYear)

      // variables HOCON para el proyectoConfigConstants.
      //procesamiento/src/test/resources/config/applicationLocal.conf , desarrollo en local se hace en test.
      logger.info(s"¿config es vacío? : ${config.isEmpty}" )
      logger.info(s"Contenido Dev Name en applicationLocal.conf : ${config.getString(DevName)}" )
      logger.info(s"Bienvenido a procesamiento de Datos $devName")

      val customerParquet: DataFrame = read(config.getConfig(CustomersParquet))
      val bikesDf: DataFrame = read(config.getConfig(BikesInput))
      val customerDf: DataFrame = read(config.getConfig(CustomersInput))

      println("Transformaciones de datos : ")

      val bikesfilter: DataFrame = bikesDf
        .filterBikes

      val customerFiltered: DataFrame =  customerDf
        .filterCustomers(currentYear)

      customerFiltered
        .show()

    } match {
      case Failure(e) => -1
      case Success(_) => 0
    }
  }

  override def getProcessId: String = "Engine"

}
