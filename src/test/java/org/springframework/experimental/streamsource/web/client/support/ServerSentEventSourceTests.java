package org.springframework.experimental.streamsource.web.client.support;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.experimental.streamsource.web.client.support.ServerSentEventSource.EventProcessor;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.http.converter.HttpMessageConverters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

/**
 * Unit tests for {@link ServerSentEventSource} that parse a raw event stream
 * given as a String, without a server.
 */
class ServerSentEventSourceTests {

	private static final HttpMessageConverters converters =
			HttpMessageConverters.forClient().registerDefaults().build();


	// Parsing

	@Test
	void parsesFields() throws Exception {
		List<ServerSentEvent<String>> events = read("""
				id:1
				event:message
				retry:1000
				data:hello

				""");

		assertThat(events)
				.extracting(ServerSentEvent::id, ServerSentEvent::event, ServerSentEvent::retry, ServerSentEvent::data)
				.containsExactly(tuple("1", "message", Duration.ofMillis(1000), "hello"));
	}

	@Test
	void stripsOneLeadingSpaceFromValue() throws Exception {
		List<ServerSentEvent<String>> events = read("""
				id: 1
				event: message
				retry: 1000
				data:  two spaces

				""");

		assertThat(events)
				.extracting(ServerSentEvent::id, ServerSentEvent::event, ServerSentEvent::retry, ServerSentEvent::data)
				.containsExactly(tuple("1", "message", Duration.ofMillis(1000), " two spaces"));
	}

	@Test
	void joinsMultipleDataLinesWithLineFeed() throws Exception {
		List<ServerSentEvent<String>> events = read("""
				data: line one
				data: line two

				data: trailing
				data:

				""");

		assertThat(events).extracting(ServerSentEvent::data)
				.containsExactly("line one\nline two", "trailing\n");
	}

	@Test
	void parsesMultipleEvents() throws Exception {
		List<ServerSentEvent<String>> events = read("""
				id: 1
				data: one

				id: 2
				data: two

				""");

		assertThat(events)
				.extracting(ServerSentEvent::id, ServerSentEvent::data)
				.containsExactly(tuple("1", "one"), tuple("2", "two"));
	}

	@Test
	void ignoresCommentLines() throws Exception {
		List<ServerSentEvent<String>> events = read("""
				: comment
				data: one
				:another comment

				""");

		assertThat(events).extracting(ServerSentEvent::data).containsExactly("one");
	}

	@Test
	void ignoresUnknownFields() throws Exception {
		List<ServerSentEvent<String>> events = read("""
				foo: bar
				data: one

				""");

		assertThat(events).extracting(ServerSentEvent::data).containsExactly("one");
	}

	@Test
	void whitespaceOnlyLineDoesNotEndEvent() throws Exception {
		List<ServerSentEvent<String>> events = read("data: one\n   \ndata: two\n\n");

		assertThat(events).extracting(ServerSentEvent::data).containsExactly("one\ntwo");
	}

	@Test
	void ignoresInvalidRetry() throws Exception {
		List<ServerSentEvent<String>> events = read("""
				retry: abc
				data: one

				retry: 99999999999999999999
				data: two

				retry: 1000
				retry: 1x
				data: three

				""");

		assertThat(events).extracting(ServerSentEvent::retry)
				.containsExactly(null, null, Duration.ofMillis(1000));
	}

	@Test
	void ignoresIdWithNullCharacter() throws Exception {
		List<ServerSentEvent<String>> events = read("id: bad\u0000id\ndata: one\n\n");

		assertThat(events).extracting(ServerSentEvent::id).containsExactly((String) null);
	}

	@Test
	void stripsByteOrderMarkAtStartOfStream() throws Exception {
		List<ServerSentEvent<String>> events = read("﻿id: 1\ndata: one\n\n");

		assertThat(events).extracting(ServerSentEvent::id).containsExactly("1");
	}

	@Test
	void supportsCrLfAndCrLineEndings() throws Exception {
		List<ServerSentEvent<String>> events = read("data: one\r\n\r\ndata: two\r\rdata: three\n\n");

		assertThat(events).extracting(ServerSentEvent::data).containsExactly("one", "two", "three");
	}

	@Test
	void emptyStream() throws Exception {
		assertThat(read("")).isEmpty();
	}

	@Test
	void discardsPartialEventAtEndOfStream() throws Exception {
		List<ServerSentEvent<String>> events = read("data: one\n\nid: 2\ndata: two\n");

		assertThat(events).extracting(ServerSentEvent::data).containsExactly("one");
	}

	@Test
	void skipsEventsWithoutData() throws Exception {
		List<ServerSentEvent<String>> events = read("""
				id: 1

				event: ping
				retry: 1000

				: comment only

				id: 4
				data: four

				""");

		assertThat(events)
				.extracting(ServerSentEvent::id, ServerSentEvent::event, ServerSentEvent::retry, ServerSentEvent::data)
				.containsExactly(tuple("4", null, null, "four"));
	}

	@Test
	void dispatchesEventWithEmptyDataLine() throws Exception {
		List<ServerSentEvent<String>> events = read("id: 1\ndata:\n\n");

		assertThat(events).extracting(ServerSentEvent::id, ServerSentEvent::data).containsExactly(tuple("1", ""));
	}


	// Conversion

	@Test
	void rawStringDataByDefault() throws Exception {
		List<ServerSentEvent<Object>> events = read("data: {\"name\":\"Joe\",\"age\":42}\n\n", event -> {});

		assertThat(events).extracting(ServerSentEvent::data).containsExactly("{\"name\":\"Joe\",\"age\":42}");
	}

	@Test
	void convertsToTargetType() throws Exception {
		List<ServerSentEvent<Person>> events = read(
				"data: {\"name\":\"Joe\",\"age\":42}\n\n", event -> event.setTargetType(Person.class));

		assertThat(events).extracting(ServerSentEvent::data).containsExactly(new Person("Joe", 42));
	}

	@Test
	void convertsToParameterizedTargetTypeWithMediaType() throws Exception {
		List<ServerSentEvent<List<Person>>> events = read(
				"data: [{\"name\":\"Jane\",\"age\":35},{\"name\":\"Jim\",\"age\":27}]\n\n",
				event -> event.setTargetType(new ParameterizedTypeReference<>() {}, MediaType.APPLICATION_JSON));

		assertThat(events).extracting(ServerSentEvent::data)
				.containsExactly(List.of(new Person("Jane", 35), new Person("Jim", 27)));
	}

	@Test
	void convertsByEventType() throws Exception {
		List<String> parsedEvents = new ArrayList<>();

		List<ServerSentEvent<Object>> events = read("""
				id: 1
				event: person
				data: {"name":"Joe","age":42}

				id: 2
				event: message
				data: plain text

				""", event -> {
			parsedEvents.add(event.id() + "|" + event.eventType() + "|" + event.data());
			if ("person".equals(event.eventType())) {
				event.setTargetType(Person.class);
			}
		});

		assertThat(events)
				.extracting(ServerSentEvent::id, ServerSentEvent::event, ServerSentEvent::data)
				.containsExactly(tuple("1", "person", new Person("Joe", 42)), tuple("2", "message", "plain text"));

		assertThat(parsedEvents).containsExactly(
				"1|person|{\"name\":\"Joe\",\"age\":42}", "2|message|plain text");
	}

	@Test
	void emptyDataWithTargetType() throws Exception {
		List<ServerSentEvent<Person>> events = read("id: 1\ndata:\n\n", event -> event.setTargetType(Person.class));

		assertThat(events).extracting(ServerSentEvent::id, ServerSentEvent::data).containsExactly(tuple("1", null));
	}


	private static List<ServerSentEvent<String>> read(String body) throws Exception {
		return readAll(createSource(body));
	}

	private static <T> List<ServerSentEvent<T>> read(String body, EventProcessor<T> processor) throws Exception {
		return readAll(new ServerSentEventSource<>(URI.create("/sse"), toInputStream(body), converters, processor));
	}

	private static ServerSentEventSource<String> createSource(String body) {
		return new ServerSentEventSource<>(URI.create("/sse"), toInputStream(body));
	}

	private static ByteArrayInputStream toInputStream(String body) {
		return new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8));
	}

	private static <T> List<ServerSentEvent<T>> readAll(ServerSentEventSource<T> source) throws Exception {
		try (source) {
			List<ServerSentEvent<T>> events = new ArrayList<>();
			while (source.receiveNext()) {
				events.add(source.next());
			}
			return events;
		}
	}


	record Person(String name, int age) {
	}

}
