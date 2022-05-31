package procesos.transormations

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.IntegerType
import procesos.common.Fields.BikesFields.{BikeId, SizeColumn}
import procesos.common.Fields.CustomerFields.{PurchaseCityColumn, PurchaseYearColumn}
import procesos.common.StaticVals.{SizeS, TokioString}

package object transformations {

  implicit class BikesDf(df: DataFrame) {
    def filterBikes: DataFrame = {
      df.
        filter(col(SizeColumn) =!= SizeS)
    }
    def groupbyBikes: DataFrame = {
      df.
        groupBy(SizeColumn).count()
    }
    def getDf: DataFrame = df

  }

  implicit class CustomerDf(df: DataFrame) {
    def filterCustomers(currentYear:String): DataFrame = {
      df.
        filter(
          (col(PurchaseCityColumn) =!= TokioString) &&
            lit(currentYear).cast(IntegerType) - col(PurchaseYearColumn).cast(IntegerType) <= 10)
    }
    def groupbyYears: DataFrame = {
      df
        .groupBy(PurchaseYearColumn,PurchaseCityColumn)
        .count()
        .sort(PurchaseYearColumn)
    }
    def getDf: DataFrame = df
  }

  implicit class CustomersBikesDf(df: DataFrame) {
    def joinCustomersBikes(bikesDf: BikesDf): CustomersBikesDf = {
      df.join(bikesDf.getDf, Seq(BikeId))
    }
    def appendColumn(column: Column): CustomersBikesDf = {
      df.select(df.columns.map(col):+ column: _*)
    }
    def getDf: DataFrame = df
  }

}
