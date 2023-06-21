package org.neoflex.source

import io.circe.Decoder
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.streaming.api.scala.{ DataStream, StreamExecutionEnvironment }
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.neoflex.Const.Kafka
import org.neoflex.serde.Serde

import scala.reflect.{ classTag, ClassTag }

sealed trait KafkaConsumerSource {

  private def configure[A: Decoder: ClassTag](bootstrapServers: String, topicName: String): KafkaSource[A] = {
    KafkaSource
      .builder()
      .setBootstrapServers(bootstrapServers)
      .setGroupId(s"$topicName-group")
      .setTopics(topicName)
      .setDeserializer(new KafkaJsonRecordDeserializer[A])
      .setStartingOffsets(OffsetsInitializer.earliest())
      .build()
  }
}

object KafkaConsumerSource extends KafkaConsumerSource {

  def configureKafkaDataSource[A: Decoder: ClassTag](
    topicName: String
  )(implicit env: StreamExecutionEnvironment,
    typeInformation: TypeInformation[A]
  ): DataStream[A] = {
    val kafkaSource = KafkaConsumerSource.configure[A](Kafka.BootstrapServers, topicName)
    env.fromSource[A](kafkaSource, WatermarkStrategy.noWatermarks[A](), s"$topicName-flow")
  }
}

class KafkaJsonRecordDeserializer[A: Decoder: ClassTag] extends KafkaRecordDeserializationSchema[A] {

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]], out: Collector[A]): Unit = {
    val value = new String(record.value())
    val obj   = Serde.toObj(value).toOption
    obj.foreach(out.collect)
  }

  override def getProducedType: TypeInformation[A] = {
    TypeExtractor
      .getForClass(classTag[A].runtimeClass.asInstanceOf[Class[A]])
      .asInstanceOf[TypeInformation[A]]
  }
}
