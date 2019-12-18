/*
 * Copyright 2006-2007 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.springsource.open.foo.async;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import com.springsource.open.foo.Handler;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.Lifecycle;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import static org.junit.Assert.assertTrue;

@SpringBootTest("spring.kafka.consumer.group-id=group")
public abstract class AbstractAsynchronousMessageTriggerTests implements ApplicationContextAware {

	@Autowired
	protected KafkaTemplate<Object, String> kafkaTemplate;

	@Autowired
	private ConsumerFactory<Object, String> consumerFactory;

	@Autowired
	private Handler handler;

	protected JdbcTemplate jdbcTemplate;

	private Lifecycle lifecycle;

	@Autowired
	public void setDataSource(@Qualifier("dataSource") DataSource dataSource) {
		this.jdbcTemplate = new JdbcTemplate(dataSource);
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.lifecycle = (Lifecycle) applicationContext;
	}

	@BeforeEach
	public void clearData() {
		// Start the listeners...
		lifecycle.start();
		getMessages(); // drain queue
		handler.resetItemCount();
		jdbcTemplate.update("delete from T_FOOS");
	}

	@AfterEach
	public void waitForMessages() throws Exception {

		int count = 0;
		while (handler.getItemCount() < 2 && (count++) < 30) {
			Thread.sleep(100);
		}
		// Stop the listeners...
		lifecycle.stop();
		// Give it time to finish up...
		Thread.sleep(2000);
		assertTrue("Wrong item count: " + handler.getItemCount(), handler.getItemCount() >= 2);

		checkPostConditions();

	}

	protected abstract void checkPostConditions();

	protected List<String> getMessages() {
		Consumer<Object, String> consumer = consumerFactory.createConsumer();
		consumer.subscribe(Pattern.compile("async"));
		ConsumerRecords<Object, String> records = consumer.poll(Duration.ofSeconds(10));
		List<String> msgs = new ArrayList<String>();
		for (Iterator<ConsumerRecord<Object, String>> iter = records.iterator(); iter.hasNext();) {
			ConsumerRecord<Object, String> record = iter.next();
			msgs.add(record.value());
		}
		consumer.close();
		return msgs;
	}

}
