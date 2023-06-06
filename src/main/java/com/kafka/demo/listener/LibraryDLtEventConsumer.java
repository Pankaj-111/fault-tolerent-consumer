package com.kafka.demo.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class LibraryDLtEventConsumer {

//	@KafkaListener(topics = "library-events")
//	public void consumeBatchOfMessages(List<ConsumerRecord<Integer, String>> message) {
//		log.info(" Message :{}", message);
//		consumerService.processRecord(message);
//	}

	@KafkaListener(topics = "library-events.DLT", groupId = "lib-dlt-grp")
	public void consumeBatchOfMessages(ConsumerRecord<Integer, String> message) {
		log.info(" ************************* DLT EVENT Message :{}", message);
		Headers headers = message.headers();
		for (Header header : headers) {
			log.info(" DLT Message Header:- key : {} and value : {}", header.key(), new String(header.value()));
		}
	}
}
