package com.springsource.open.foo;

import java.util.Collections;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class FooHandler implements Handler {

	private static final Log log = LogFactory.getLog(FooHandler.class);

	private final JdbcTemplate jdbcTemplate;

	private final AtomicInteger count = new AtomicInteger(0);

	private KafkaTemplate<Object, String> kafkaTemplate;

	public FooHandler(DataSource dataSource, KafkaTemplate<Object, String> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
		this.jdbcTemplate = new JdbcTemplate(dataSource);
	}

	@Override
	public void handle(String msg) {

		log.debug("Received message: [" + msg + "]");

		Date date = new Date();
		jdbcTemplate.update("INSERT INTO T_FOOS (ID, name, foo_date) values (?, ?,?)", count.getAndIncrement(), msg,
				date);

		log.debug(String.format("Inserted foo with name=%s, date=%s", msg, date));

	}

	@KafkaListener(topics = "async")
	public void handle(ConsumerRecord<Object, String> record) {
		handle(record.value());
		kafkaTemplate.sendOffsetsToTransaction(Collections.singletonMap(
				new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1)));
	}

	@Override
	public void resetItemCount() {
		count.set(0);
	}

	@Override
	public int getItemCount() {
		return count.get();
	}

}
