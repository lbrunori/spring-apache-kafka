package com.github.lbrunori;

import java.util.Properties;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

@Slf4j
public class ProducerDemo {

  public static void main(String[] args) {
    String bootstrapServers = "127.0.0.1:9092";

    var properties = new Properties();

    // create Producer properties
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // create the producer
    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

    // create a producer record
    // send data
    IntStream.range(0, 1000).boxed().forEach(i -> {
      ProducerRecord<String, String> record = new ProducerRecord<>("first-topic", "hello world" + i);
      producer.send(record, (recordMetadata, e) -> {
        if (e == null) {
          //the record was sent
          log.info("Topic {}", recordMetadata.topic());
          log.info("Offset {}", recordMetadata.offset());
          log.info("Partition {}", recordMetadata.partition());
          log.info("Timestamp {}", recordMetadata.timestamp());
        } else {
          log.error("Error {}", e.getMessage());
        }
      });
    });

    //flush data
    producer.flush();
    //flush and close the producer
    producer.close();
  }
}
