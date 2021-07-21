package com.github.ml2dkanth.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaProducerCallbackDemo {
    public static void main(String[] args) {
        // Create a Slf4J logger
        Logger logger= LoggerFactory.getLogger(KafkaProducerCallbackDemo.class);

        // Properties for Kafka Producer
        String bootstrapServer="127.0.0.1:9092";
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //Create a Kafka Producer
        KafkaProducer<String,String> kproducer =new KafkaProducer<String, String>(properties);


        // Procedure for create ProducerRecord
        ProducerRecord<String,String> record1;

        for(int i=0;i<10;i++) {
            record1 = new ProducerRecord<String, String>("first_topic", "Hello Srikanth " + i);

            //Producer send the data
            kproducer.send(record1, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("\nReceived Metadata Record: \n" +
                                "Topic : " + recordMetadata.topic() + "\n" +
                                "Partition : " + recordMetadata.partition() + "\n" +
                                "Offset : " + recordMetadata.offset() + "\n" +
                                "Timestamp : " + recordMetadata.timestamp());
                    } else {
                        System.out.println("Exception : Not able to fetch, " + e.getMessage());
                    }
                }
            });

        }
        kproducer.flush();
        kproducer.close();

    }
}
