package procesos.common.output
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{col, count, sum, when}
import procesos.common.Field
import procesos.transormations.UdafAgg

object CustomersBikes {

  val w:WindowSpec = Window.partitionBy("name")

  case object NBikes extends Field {

    override val name: String = "n_bikes"

    def apply:Column = { count("*").over(w).alias(name) }

  }

  case object TotalSpend extends Field {

    override val name: String = "total_spent"

    def apply:Column = { sum("price").over(w).alias(name) }

  }

  case object TotalOnline extends Field {

    override val name: String = "total_online"

    def apply:Column = { sum(when(col("purchase_online"), 1).otherwise(0)).over(w).alias(name) }

  }

  case object UdafCustomeAgg extends Field {

    override val name: String = "avg_udaf"

    def apply: Column = {
      UdafAgg(col("price")).over(w).alias(name)
    }

  }

}
