package com.notificationservice.kafka;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaProducerConfig {

    @Bean
    public KafkaNotificationProducer kafkaNotificationProducer() {
        return new KafkaNotificationProducer();
    }
}
