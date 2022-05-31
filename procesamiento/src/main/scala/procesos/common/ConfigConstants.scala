package procesos.common

object ConfigConstants {

  val RootConfig: String = "procesamientoJob"
  val ParamsConfig: String = s"$RootConfig.params"
  val InputConfig: String = s"$RootConfig.input"

  val DevName:String = s"$ParamsConfig.devName"
  val CurrentYear: String = s"$ParamsConfig.currentYear"

  val CustomersParquet:String = s"$InputConfig.fdevCustomersParquet"

  val BikesInput:String = s"$InputConfig.fdevBikes"
  val CustomersInput:String = s"$InputConfig.fdevCustomers"

}
