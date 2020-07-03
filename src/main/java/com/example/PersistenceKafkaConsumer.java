package com.example;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;


@Component
@ConditionalOnBean( name = "kafkaConsumerConfiguration" )
public class PersistenceKafkaConsumer
{
    private static final Logger LOGGER = LoggerFactory.getLogger( PersistenceKafkaConsumer.class );


    @KafkaListener( id = "batch-listener-0", topics = "#{topicsConfiguration.persistence.name}", groupId = "cm-persistence-notification", containerFactory = KafkaConsumerConfiguration.CONTAINER_FACTORY_NAME )
    public void receive(
        @Payload List<String> messages,
        @Header( KafkaHeaders.RECEIVED_MESSAGE_KEY ) List<String> keys,
        @Header( KafkaHeaders.RECEIVED_PARTITION_ID ) List<Integer> partitions,
        @Header( KafkaHeaders.RECEIVED_TOPIC ) List<String> topics,
        @Header( KafkaHeaders.OFFSET ) List<Long> offsets,
        Acknowledgment ack )
    {
        long startTime = System.currentTimeMillis();

        handleNotifications( messages );

        long endTime = System.currentTimeMillis();

        long timeElapsed = endTime - startTime;

        LOGGER.info( "Execution Time :{}", timeElapsed );

        ack.acknowledge();

        LOGGER.info( "Acknowledgment Success" );

    }


    public void handleNotifications( List<String> messages )
    {
        LOGGER.info( "beginning to consume batch messages , Message Count :{}", messages.size() );

        // db insertion in handle notification , will take more than 7 seconds
        // to update db
    }
}
