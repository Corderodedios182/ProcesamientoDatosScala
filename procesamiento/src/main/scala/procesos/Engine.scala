package procesos

import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.typesafe.scalalogging.LazyLogging

class Engine extends SparkProcess with LazyLogging {

  override def runProcess(runtimeContext: RuntimeContext): Int = {1000}

  override def getProcessId: String = "Engine"

}
