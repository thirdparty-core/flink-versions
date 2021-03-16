package udf.udf

import org.apache.flink.table.annotation.DataTypeHint
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

/**
 * 详情请参考：
 * https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/functions/udfs.html#automatic-type-inference
 */
class StructDer extends ScalarFunction {

  @DataTypeHint(value = "ROW<f1 STRING, f2 INT, f3 BIGINT>")
  def eval(@DataTypeHint(value = "BYTES") bytes: Array[Byte]): Row = {
    val struct = Struct.deserialize(bytes)
    Row.of(struct.field0, Integer.valueOf(struct.field1), java.lang.Long.valueOf(struct.field2))
  }

  // 方式2：
  // @DataTypeHint(value = "RAW", bridgedTo = classOf[Struct])
  // def eval(@DataTypeHint(value = "BYTES") bytes: Array[Byte]): Struct = {
  //   Struct.deserialize(bytes)
  // }

  // 方式2：
  // @DataTypeHint(value = "RAW", bridgedTo = classOf[Row])
  // def eval(@DataTypeHint(value = "BYTES") bytes: Array[Byte]): Row = {
  //   val struct = Struct.deserialize(bytes)
  //   Row.of(struct.field0, Integer.valueOf(struct.field1), java.lang.Long.valueOf(struct.field2))
  // }
}