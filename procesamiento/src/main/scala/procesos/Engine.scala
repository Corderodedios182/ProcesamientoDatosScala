package procesos

import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.datio.dataproc.sdk.schema.exception.DataprocSchemaException.InvalidDatasetException
import com.typesafe.scalalogging.LazyLogging
import com.typesafe.config.Config
import procesos.common.ConfigConstants

import scala.util.{Failure, Success, Try}

class Engine extends SparkProcess with LazyLogging {

  //RuntimeContext : Contiene algunas cosas para la ejecución
  override def runProcess(runtimeContext: RuntimeContext): Int  = {

    Try {

      logger.info(s"Process Id : ${runtimeContext.getProcessId}") //Arroja el getProcessID : Engine

      val config: Config = runtimeContext.getConfig

      logger.info(s"¿config es vacío? : ${config.isEmpty}" )
      logger.info(s"Contenido de config : ${config.getString(ConfigConstants.DevName)}" )

    } match {
      case Success(_) => 0
      case Failure(exception: InvalidDatasetException) => {
        for (err <- exception.getErrors.toArray) {
          logger.error(err.toString)
        }
        100
      }
      case Failure(exception: Exception) => {
        exception.printStackTrace()
        100
      }
    }
  }

  override def getProcessId: String = "Engine"

}
