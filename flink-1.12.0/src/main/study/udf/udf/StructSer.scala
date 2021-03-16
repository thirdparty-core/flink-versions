package udf.udf

import org.apache.flink.table.annotation.DataTypeHint
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

/**
 * 详情请参考：
 * https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/functions/udfs.html#automatic-type-inference
 */
class StructSer extends ScalarFunction {

  @DataTypeHint(value = "BYTES")
  def eval(@DataTypeHint(value = "ROW<f1 STRING, f2 INT, f3 BIGINT>") row: Row): Array[Byte] = {
    val f0 = row.getField(0).asInstanceOf[String]
    val f1 = row.getField(1).asInstanceOf[Int]
    val f2 = row.getField(2).asInstanceOf[Long]
    Struct.serialize(Struct(f0, f1, f2))
  }

  // 方式2
  // @DataTypeHint(value = "BYTES")
  // def eval(@DataTypeHint(inputGroup = InputGroup.ANY) obj: Object): Array[Byte] = {
  //   val row = obj.asInstanceOf[Row]
  //   val f0 = row.getField(0).asInstanceOf[String]
  //   val f1 = row.getField(1).asInstanceOf[Int]
  //   val f2 = row.getField(2).asInstanceOf[Long]
  //   Struct.serialize(Struct(f0, f1, f2))
  // }

}
