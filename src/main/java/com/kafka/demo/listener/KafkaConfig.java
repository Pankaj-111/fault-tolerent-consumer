package com.kafka.demo.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
@EnableKafka
public class KafkaConfig {

	private static final String RETRYSENT_ATTEMPT = "RETRYSENT_ATTEMPT";

	@Autowired
	private KafkaTemplate<?, ?> template;

	@Bean
	public ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
			final ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
			final ConsumerFactory<Object, Object> kafkaConsumerFactory) {
		final ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
		configurer.configure(factory, kafkaConsumerFactory);
//		factory.setConcurrency(3);
//		factory.setBatchListener(true);
//        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
		factory.setCommonErrorHandler(errorHandler());
		return factory;
	}

	public DeadLetterPublishingRecoverer recoverer() {
		final DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template,
				(consumerRecord, exception) -> {
					final int retrySent = getRetrySentCount(consumerRecord) + 1;
					final String retryCount = retrySent + "";
					log.info("{} Retries in topic ", retrySent);
					if (exception.getCause() instanceof RecoverableDataAccessException && retrySent <= 1) {
						log.info("Sending message to RETRY topic :{}", consumerRecord);
						updateRetryCountHeader(consumerRecord, retryCount);
						return new TopicPartition("library-events.RETRY", consumerRecord.partition());
					} else {
						log.info("Sending message to DLT topic :{}", consumerRecord);
						updateRetryCountHeader(consumerRecord, retryCount);
						return new TopicPartition("library-events.DLT", consumerRecord.partition());
					}
				});
		return recoverer;
	}

	public DefaultErrorHandler errorHandler() {
//		var fixedBackOff = new FixedBackOff(1000L, 2);
//		var errorHandler = new DefaultErrorHandler(fixedBackOff);
		final ExponentialBackOffWithMaxRetries exOffWithMaxRetries = new ExponentialBackOffWithMaxRetries(2);
		exOffWithMaxRetries.setInitialInterval(1_000L);
		exOffWithMaxRetries.setMultiplier(2);
		exOffWithMaxRetries.setMaxInterval(2_000);
		var errorHandler = new DefaultErrorHandler(recoverer(), exOffWithMaxRetries);

		errorHandler.setRetryListeners(((record, ex, deliveryAttempt) -> {
			log.info("Failed Record in Retry Listener, Exception : {} , deliveryAttempt : {} ", ex.getMessage(),
					deliveryAttempt);
		}));
		return errorHandler;
	}

	private void updateRetryCountHeader(final ConsumerRecord<?, ?> consumerRecord, final String retryCount) {
		consumerRecord.headers().remove(RETRYSENT_ATTEMPT);
		consumerRecord.headers().add(RETRYSENT_ATTEMPT, retryCount.getBytes());
	}

	private int getRetrySentCount(final ConsumerRecord<?, ?> consumerRecord) {
		final Headers headers = consumerRecord.headers();
		for (Header header : headers) {
			if (header.key().equalsIgnoreCase(RETRYSENT_ATTEMPT)) {
				return Integer.valueOf(new String(header.value())).intValue();
			}
		}
		return 0;
	}

}
