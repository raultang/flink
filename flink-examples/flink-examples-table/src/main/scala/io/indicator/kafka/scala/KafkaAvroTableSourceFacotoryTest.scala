package io.indicator.kafka.scala

import org.apache.flink.table.sources.TableSourceFactoryService

import scala.collection.mutable

/**
  * Created by ytang3 on 6/21/18.
  */
object KafkaAvroTableSourceFacotoryTest {

  def testValidProperties() = {
    val props = properties()
    TableSourceFactoryService.findAndCreateTableSource(props.toMap)
  }

  def properties() : mutable.Map[String,String] = {
    val properties = mutable.Map[String, String]()
    properties.put("connector.properties.0.key", "bootstrap.servers")
    properties.put("connector.properties.0.value", "rheos-bh-stg-agg-kfk-1.lvs02.dev.ebayc3.com:9092")
    properties.put("connector.properties.1.key", "group.id")
    properties.put("connector.properties.1.value", "ty_test")
    properties.put("connector.property-version", "1")
    properties.put("connector.startup-mode", "earliest-offset")
    properties.put("connector.topic", "behavior.pulsar.sojevent.total")
    properties.put("connector.type", "kafka")
    properties.put("connector.version", "0.10")
    properties.put("format.record-class", "SojEvent")
    properties.put("format.type", "avro")
    properties.put("schema.0.name", "guid")
    properties.put("schema.0.type", "VARCHAR")
    properties.put("schema.1.name", "eventTimestamp")
    properties.put("schema.1.type", "BIGINT")
    properties.put("schema.2.name", "pageId")
    properties.put("schema.2.type", "INT")
    properties
  }

  def main(args: Array[String]): Unit = {
    testValidProperties()
  }

}
