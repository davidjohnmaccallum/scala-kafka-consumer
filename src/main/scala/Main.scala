import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util._
import scala.collection.immutable.LazyList.cons

object Main extends App {
  
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("auto.offser.reset", "latest")
  props.put("group.id", "consumer-group")
  val consumer = new KafkaConsumer[String, String](props)
  consumer.subscribe(Arrays.asList("davidstopic"))
  while (true) {
    val record = consumer.poll(1000)
    record.forEach(consumer => println(consumer.value()))
  }

}