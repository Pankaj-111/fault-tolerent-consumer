package com.kafka.demo.listener;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class LibararyConsumer {
	@Autowired
	private ConsumerService consumerService;
	
//	@KafkaListener(topics = "library-events")
	public void consumeBatchOfMessages(List<ConsumerRecord<Integer, String>> message) {
		log.info(" Message :{}", message);
		consumerService.processRecord(message);
	}
	
	@KafkaListener(topics = "library-events")
	public void consumeBatchOfMessages(ConsumerRecord<Integer, String> message) {
		log.info(" Original Message :{}", message);
		consumerService.processRecord(message);
	}
}
