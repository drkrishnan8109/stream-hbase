package com.model

case class Location(latitude: Double, longitude: Double)
case class Data(
    deviceId: String,
    temperature: Int,
    location: Location,
    time: Long
)
case class IotInfo(data: Data)

object Data extends Serializable {

  import org.apache.hadoop.hbase.client.Put
  import org.apache.hadoop.hbase.io.ImmutableBytesWritable
  import org.apache.hadoop.hbase.util.Bytes

  def convertToPut(event: Data): (ImmutableBytesWritable, Put) = {
    val rowkey = event.deviceId + "_" + event.time
    val p = new Put(Bytes.toBytes(rowkey))

    p.addColumn(
      Bytes.toBytes("data"),
      Bytes.toBytes("deviceId"),
      Bytes.toBytes(event.deviceId)
    )
    p.addColumn(
      Bytes.toBytes("data"),
      Bytes.toBytes("temperature"),
      Bytes.toBytes(event.temperature)
    )
    p.addColumn(
      Bytes.toBytes("data"),
      Bytes.toBytes("time"),
      Bytes.toBytes(event.time) //convert to readable
    )
    p.addColumn(
      Bytes.toBytes("location"),
      Bytes.toBytes("latitude"),
      Bytes.toBytes(event.location.latitude) // or latitude, longitude ?
    )
    p.addColumn(
      Bytes.toBytes("location"),
      Bytes.toBytes("longitude"),
      Bytes.toBytes(event.location.longitude) // or latitude, longitude ?
    )

    return (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), p)
  }
}
