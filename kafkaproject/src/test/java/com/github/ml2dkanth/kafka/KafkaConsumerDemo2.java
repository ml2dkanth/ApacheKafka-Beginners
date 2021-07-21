package com.github.ml2dkanth.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class KafkaConsumerDemo2 {
    public static void main(String[] args) {
        //Create logger for the class
        Logger logger = LoggerFactory.getLogger(KafkaConsumerDemo2.class.getName());

        //Config Properties
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-first-application";
        //Create Properties for the consumer
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Create kafka Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //Subscribe Consumer to a topic
        consumer.subscribe(Collections.singleton("first_topic"));
        logger.info("Entering consumer poll\n");
        // Poll for the data
        int limit = 0;

        while (limit < 5) {

            final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            int counter=records.count();
            logger.info("Total Number Records # :"+counter+"\n");

            for (ConsumerRecord<String, String> record : records)
            {
                logger.info("Topic : "+record.topic());
                logger.info("Partition : "+record.partition());
                logger.info("Offset : "+record.offset());
                logger.info("Key : "+record.key()+" "+record.value());

            }
            logger.info("\nimit # "+limit);
            limit++;
        }
        logger.info("Exiting consumer poll");
    }
}
