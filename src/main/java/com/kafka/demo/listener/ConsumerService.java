package com.kafka.demo.listener;

import java.util.List;
import java.util.Random;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.kafka.demo.domain.LibraryEvent;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class ConsumerService {
	private static final String INVALID_EVENT_ID = "Invalid event id";
	private final Gson gson = new Gson();

	public void processRecord(List<ConsumerRecord<Integer, String>> message) {
		final ConsumerRecord<Integer, String> record = message.get(0);
		log.info("Consumer Record : {}", record);
		final LibraryEvent event = gson.fromJson(record.value().toString(), LibraryEvent.class);
		log.info("Event Object : {}", event);
		if (event.getEventId() == -99) {
			throw new RecoverableDataAccessException(INVALID_EVENT_ID);
		}
	}

	public void processRecord(ConsumerRecord<Integer, String> message) {
		final ConsumerRecord<Integer, String> record = message;
		log.info("Consumer Record : {}", record);
		final LibraryEvent event = gson.fromJson(record.value().toString(), LibraryEvent.class);
		log.info("Event Object : {}", event);
		int num = getRandom();
		log.info("Exception flag :{}", num);
		if (event.getEventId() == -99
				&& (num == 1 || num == 2 || num == 3 || num == 4 || num == 8 || num == 5 || num == 6 || num == 7 || num == 9 )) {
			throw new RecoverableDataAccessException(INVALID_EVENT_ID);
		}

		log.info("Event Object procesed successfully : {}", event);
	}

	private int getRandom() {
		int max = 10;
		int min = 0;
		Random random = new Random();
		return random.nextInt(max - min + 1) + min;
	}
}
