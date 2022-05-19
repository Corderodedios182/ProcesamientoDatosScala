package procesos

import com.bbva.datioamproduct.fdevdatio.utils.IOUtils
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.typesafe.config.Config
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

    } match {
      case Failure(e) => -1
      case Success(_) => 0
    }
  }

  override def getProcessId: String = "Engine"

}
