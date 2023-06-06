package com.kafka.demo.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class LibararyRetryConsumer {
	@Autowired
	private ConsumerService consumerService;

//	@KafkaListener(topics = "library-events")
//	public void consumeBatchOfMessages(List<ConsumerRecord<Integer, String>> message) {
//		log.info(" Message :{}", message);
//		consumerService.processRecord(message);
//	}

	@KafkaListener(topics = "library-events.RETRY", groupId = "lib-retry-grp")
	public void consumeBatchOfMessages(ConsumerRecord<Integer, String> message) {
		log.info(" ************************* RETRY Message :{}", message);
		Headers headers = message.headers();
		for (Header header : headers) {
			log.info("Retry Header:- key : {}, value : {}", header.key(), new String(header.value()));
		}
		consumerService.processRecord(message);
	}
}
