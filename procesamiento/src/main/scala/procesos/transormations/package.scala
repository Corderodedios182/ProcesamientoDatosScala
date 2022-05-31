package procesos.transormations

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, lit, udf, when}
import org.apache.spark.sql.types.IntegerType
import procesos.common.Fields.BikesFields.{BikeId, SizeColumn}
import procesos.common.Fields.CustomerFields.{PurchaseCityColumn, PurchaseYearColumn}
import procesos.common.StaticVals.{SizeS, TokioString}

package object transformations {

  implicit class BikesDf(df: DataFrame) {

    def filterBikes:BikesDf = {df.filter(col(SizeColumn) =!= SizeS)}

    def gpBikes:BikesDf = {df.groupBy(SizeColumn).count()}

    def getDf:DataFrame = df

  }

  implicit class CustomerDf(df: DataFrame) {

    def filterCustomers(currentYear:String): CustomerDf = {df.filter((col(PurchaseCityColumn) =!= TokioString) &&
                                                                     lit(currentYear).cast(IntegerType) - col(PurchaseYearColumn).cast(IntegerType) <= 10) }

    def gpYears: CustomerDf = {df.groupBy(PurchaseYearColumn,PurchaseCityColumn)
                                     .count()
                                     .sort(PurchaseYearColumn) }

    def getDf: DataFrame = df

  }

  implicit class CustomersBikesDf(df: DataFrame) {

    def joinCustomersBikes(bikesDf: BikesDf): CustomersBikesDf = { df.join(bikesDf.getDf, Seq(BikeId)) }

    def appendColumn(column: Column): CustomersBikesDf = { df.select(df.columns.map(col):+ column: _*) }

    def appendCheapColumn: CustomersBikesDf = {

      val cheapColumn: Column = when(col("price") < 8000, lit("A"))
                               .when(col("price") <= 10000, "B")
                               .otherwise("C")
                               .alias("cheap")

      df.select( df.columns.map(name => col(name)) :+ cheapColumn :_* )

    }

    def appendCheapUDF: CustomersBikesDf = {
      //Este ejemplo es similar a la lógica con when, no es recomendable su uso por tema de performances.
      val udfFunction = udf {
        (price: Int) => {
          price match {
            case  price:Int if price < 8000 => "A"
            case  price:Int if price <= 10000 => "B"
            case  _ => "C"
          }
        }
      }

      val cheapColumn: Column = udfFunction(col("price")).alias("cheap_udf")

      df.select( df.columns.map(name => col(name)) :+ cheapColumn :_* )

    }

    def getDf: DataFrame = df

  }

}
