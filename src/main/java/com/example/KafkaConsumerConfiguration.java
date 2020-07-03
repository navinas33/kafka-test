package com.example;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.support.LogIfLevelEnabled;


@Configuration
@ConditionalOnProperty( name = "kafka.enabled", matchIfMissing = true )
@EnableKafka
public class KafkaConsumerConfiguration
{
    public static final String CONTAINER_FACTORY_NAME = "cmPersistenceListenerContainerFactory";


    @Bean( CONTAINER_FACTORY_NAME )
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> cmPersistenceListenerContainerFactory(
        KafkaProperties kafkaProperties )
    {
        ConcurrentKafkaListenerContainerFactory<String, String> containerFactory =
            new ConcurrentKafkaListenerContainerFactory<>();

        Map<String, Object> consumerProperties = kafkaProperties.buildConsumerProperties();
        consumerProperties.put( ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100" );
        consumerProperties.put( ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false );
        consumerProperties.put( ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false );

        // consumerProperties.put( ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
        // "earliest" );
        containerFactory
            .setConsumerFactory(
                new DefaultKafkaConsumerFactory<>(
                    consumerProperties, new StringDeserializer(), new StringDeserializer() ) );
        containerFactory.setBatchListener( true );

        // containerFactory.getContainerProperties().setAckOnError(false);
        // containerFactory.getContainerProperties().setErrorHandler(new
        // SeekToCurrentErrorHandler());
        // containerFactory.getContainerProperties().setAckMode(AckMode.RECORD);

        containerFactory.getContainerProperties().setCommitLogLevel( LogIfLevelEnabled.Level.INFO );

        containerFactory.getContainerProperties().setAckMode( AckMode.MANUAL_IMMEDIATE );
        // containerFactory.setConcurrency( 3 );

        return containerFactory;
    }


    @Bean
    public KafkaAdmin kafkaAdmin( KafkaProperties kafkaProperties )
    {
        return new KafkaAdmin( kafkaProperties.buildAdminProperties() );
    }

}
