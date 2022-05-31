package procesos.common.output
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{count, sum}
import procesos.common.Field

object CustomersBikes {

  val w:WindowSpec = Window.partitionBy("name")

  case object NBikes extends Field {
    override val name: String = "n_bikes"

    def apply:Column = {
      count("*").over(w).alias(name)
    }
  }

  case object TotalSpend extends Field {
    override val name: String = "total_spent"

    def apply:Column = {
      sum("price").over(w).alias(name)
    }
  }

}
