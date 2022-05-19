package procesos.transormations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import procesos.common.Fields.BikesFields.SizeColumn
import procesos.common.Fields.CustomerFields.{PurchaseCityColumn, PurchaseYearColumn}
import procesos.common.StaticVals.{SizeS, TokioString}

package object packages {

  implicit class BikesDf(df: DataFrame) {
    def filterBikes: DataFrame = {
      df.
        filter(col(SizeColumn) =!= SizeS)
    }

    def groupbyBikes: DataFrame = {
      df.
        groupBy(SizeColumn).count()
    }

  }

  implicit class CustomerDf(df: DataFrame) {
    def filterCustomers: DataFrame = {
      df.
        filter((col(PurchaseCityColumn) =!= TokioString) && (col(PurchaseYearColumn) > 2010))
    }

    def groupbyYears: DataFrame = {
      df
        .groupBy(PurchaseYearColumn,PurchaseCityColumn)
        .count()
        .sort(PurchaseYearColumn)
    }

  }

}
