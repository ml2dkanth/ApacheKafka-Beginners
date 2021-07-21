package com.github.ml2dkanth.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducerKeysDemo {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // Create a Slf4J logger
        Logger logger= LoggerFactory.getLogger(KafkaProducerKeysDemo.class);

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

        //Send data multiple times
        for(int i=0;i<10;i++) {

            String mtopic="first_topic";
            String value="Hello Srikanth#"+i;
            String mkey="id_"+i;

            record1 = new ProducerRecord<>(mtopic,mkey,value);

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
            }).get();

        }
        kproducer.flush();
        kproducer.close();

    }
}
