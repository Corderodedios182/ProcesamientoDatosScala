package procesos

import com.datio.dataproc.sdk.launcher.SparkLauncher
import org.slf4j.{Logger, LoggerFactory}

object Launcher extends App {

  private val logger = LoggerFactory.getLogger(this.getClass)

  if (args.length == 0) {
    logger.error("Parametros de configuracion en un archivo en path obligatorios, ver editar configuración y ver las variables faltantes.")
    System.exit(1000)
  }
  SparkLauncher.main(Array(args(0), "Engine"))
}
