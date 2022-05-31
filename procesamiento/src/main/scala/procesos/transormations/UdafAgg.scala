package procesos.transormations

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, LongType, StructField, StructType}

//Funciones de agregación definidas por el Usuario.

object UdafAgg extends UserDefinedAggregateFunction {

  /**
   * Estructura de campos que recibe nuestra función.
   */

  override def inputSchema: StructType = StructType(
    Seq(
      StructField("price", IntegerType)
    )
  )

  /**
   * Estructura de la salida en una operación entre 2 registros.
   */

  override def bufferSchema: StructType = StructType(
    Seq(
      StructField("sum", LongType),
      StructField("counter", LongType)
    )
  )

  /**
   * Tipo de dato que retorna nuestra función
   */

  override def dataType: DataType = DoubleType

  /**
   * ¿Ante la misma entrada nuestra función retorna el mismo valor?
   */

  override def deterministic: Boolean = true

  /**
   * Definimos los valores iniciales en los campos del buffer.
   */

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 1L
  }

  /**
   * Operación que se realiza entre el buffer y un registro de entrada. (Mapeo)
   */

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Long](0) + input.getAs[Int](0)
    buffer(1) = buffer(1).asInstanceOf[Long] + 1L
  }

  /**
   * Operación entre buffers resultantes (Reducciones)
   */

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Long](0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getAs[Long](1) + buffer2.getLong(1)
  }

  /**
   * Operación final que retorna el resultado de nuestra función.
   */

  override def evaluate(buffer: Row): Any =  buffer.getLong(0).toDouble / buffer.getLong(1)

}
