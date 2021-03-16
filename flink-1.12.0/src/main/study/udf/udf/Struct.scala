package udf.udf

import java.nio.ByteBuffer
import java.util.Objects

case class Struct(field0: String, field1: Int, field2: Long)

/**
 * Struct序列化为字节数组以及反序列化
 */
object Struct {
  def serialize(struct: Struct): Array[Byte] = {
    if (Objects.isNull(struct)) return null
    val field0 = struct.field0
    var field0Bytes: Array[Byte] = new Array[Byte](0)
    if (Objects.nonNull(field0) && field0.length > 0) {
      field0Bytes = field0.getBytes
    }
    val buffer = ByteBuffer.allocate(4 + field0Bytes.length + 4 + 8)
    buffer.putInt(field0Bytes.length)
    buffer.put(field0Bytes)
    buffer.putInt(struct.field1)
    buffer.putLong(struct.field2)
    buffer.array()
  }

  def deserialize(bytes: Array[Byte]): Struct = {
    if (Objects.isNull(bytes) || bytes.length == 0)
      return null
    val buffer = ByteBuffer.wrap(bytes)
    val field0Len = buffer.getInt
    val field0Bytes = new Array[Byte](field0Len)
    buffer.get(field0Bytes)
    val field0 = new String(field0Bytes)
    val field1 = buffer.getInt
    val field2 = buffer.getLong
    Struct(field0, field1, field2)
  }
}
