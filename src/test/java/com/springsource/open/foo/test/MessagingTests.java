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

package com.springsource.open.foo.test;

import java.time.Duration;

import com.springsource.open.foo.FooHandler;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

@SpringBootTest
public class MessagingTests {

	@Autowired
	private KafkaTemplate<Object, String> kafkaTemplate;

	@Autowired
	private FooHandler consumer;

	@BeforeEach
	public void onSetUp() throws Exception {
		Thread.sleep(100L);
		kafkaTemplate.send("async", "foo");
		kafkaTemplate.send("async", "bar");
	}

	@Test
	public void testMessaging() throws Exception {
		Awaitility.waitAtMost(Duration.ofSeconds(30)).until(this.consumer::getItemCount, not(equalTo(0)));
		assertThat(consumer.getItemCount()).isEqualTo(2);
	}

}
