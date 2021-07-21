package com.github.ml2dkanth.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

public class KafkaConsumerDemo {
    public static void main(String[] args) {
        //Create logger for the class
        Logger logger = LoggerFactory.getLogger(KafkaConsumerDemo.class.getName());

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

            ConsumerRecords records = consumer.poll(Duration.ofMillis(100));
            int counter=records.count();
            logger.info("Total Number Records # :"+counter+"\n");
            final List<String> valueList = new ArrayList<>();
            records.forEach(record -> valueList.add(record.toString()));
            logger.info("Value List :"+valueList+"\n");
            int k=0;
            while (!valueList.isEmpty() && k<valueList.size()) {
                  logger.info("The Records is : "+valueList.get(k));
                  k++;
            }
            logger.info("Limit # "+limit+"\n");
            limit++;
        }
        logger.info("Exiting consumer poll\n");
    }
}
