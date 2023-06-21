package org.neoflex.testdata

import io.circe.Encoder
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerRecord }
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.neoflex.Const.Kafka
import org.neoflex.serde.Serde

import java.util.Properties

sealed class TestKafkaProducer {

  private def properties = {
    val props = new Properties()
    props.put("bootstrap.servers", Kafka.BootstrapServers)
    props.put("key.serializer", classOf[ByteArraySerializer].getName)
    props.put("value.serializer", classOf[ByteArraySerializer].getName)
    props
  }

  def produce[A: Encoder](topicName: String, obj: A): Unit = {
    val producer = new KafkaProducer[Array[Byte], Array[Byte]](properties)
    val record   = new ProducerRecord[Array[Byte], Array[Byte]](topicName, Serde.toJson(obj).getBytes)
    producer.send(record)
    producer.close()
  }
}

object TestKafkaProducer extends TestKafkaProducer
