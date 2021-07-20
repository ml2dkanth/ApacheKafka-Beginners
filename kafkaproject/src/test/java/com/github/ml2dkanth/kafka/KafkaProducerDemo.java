package com.github.ml2dkanth.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerDemo {
    public static void main(String[] args) {

        // Properties for Kafka Producer
        String bootstrapServer="127.0.0.1:9092";
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //Create a Kafka Producer
        KafkaProducer<String,String> kproducer =new KafkaProducer<String, String>(properties);

        // Create Kafka Producer Data
        // Procedure for create Producer Record
        ProducerRecord<String,String> record1, record2, record3;
        record1=new ProducerRecord<String,String>("first_topic","Hello Srikanth");
        record2=new ProducerRecord<String,String>("first_topic","Hello Akshit");
        record3=new ProducerRecord<String,String>("first_topic","Hello Sriyan");

        kproducer.send(record1);
        kproducer.send(record2);
        kproducer.send(record3);

        kproducer.flush();
        kproducer.close();

    }
}
