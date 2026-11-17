package org.springframework.experimental.streamsource.web.client.support;

import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

import http.server.HttpServer;
import http.server.TomcatHttpServer;
import io.spring.streamsource.Source;
import io.spring.streamsource.task.ActiveSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.task.VirtualThreadTaskExecutor;
import org.springframework.experimental.streamsource.web.client.support.ServerSentEventSource.EventProcessor;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.http.converter.HttpMessageConverters;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestClient;
import org.springframework.web.servlet.config.annotation.AsyncSupportConfigurer;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

/**
 * Integration tests exercising a full SSE round trip: a running HTTP server with
 * {@link SseController} endpoints, consumed via {@link ServerSentEventSource}.
 */
class SseIntegrationTests {

	private static final HttpServer server = new TomcatHttpServer();

	private static RestClient restClient;


	@BeforeAll
	static void startServer() throws Exception {
		server.register(WebConfig.class);
		server.start();
		restClient = RestClient.create(server.getBaseUrl());
	}

	@AfterAll
	static void stopServer() throws Exception {
		server.stop();
	}


	@Test
	void receivesAllEventsUntilCompletion() throws Exception {
		try (Source<ServerSentEvent<String>> source = performRequest("/sse")) {
			List<ServerSentEvent<String>> events = new ArrayList<>();
			while (source.receiveNext()) {
				events.add(source.next());
			}
			assertThat(events)
					.extracting(ServerSentEvent::id, ServerSentEvent::data)
					.containsExactly(tuple("1", "data-1"), tuple("2", "data-2"));
		}
	}


	@Test
	void closingSourceStopsReceiving() throws Exception {
		Source<ServerSentEvent<String>> source = performRequest("/sse");
		try (source) {
			assertThat(source.receiveNext()).isTrue();
			assertThat(source.next().id()).isEqualTo("1");
		}
	}

	@Test
	void emptyCompletionReturnsFalse() throws Exception {
		try (Source<ServerSentEvent<String>> source = performRequest("/sse-complete-empty")) {
			assertThat(source.receiveNext()).isFalse();
		}
	}

	@Test
	void errorCompletionPropagates() throws Exception {
		try (Source<ServerSentEvent<String>> source = performRequest("/sse-complete-with-error")) {
			assertThat(source.receiveNext()).isTrue();
			assertThat(source.next().id()).isEqualTo("1");

			assertThatThrownBy(source::receiveNext).isInstanceOf(Exception.class);
		}
	}


	@Test
	void resolvesEventsByType() throws Exception {
		List<String> rawEvents = new ArrayList<>();

		HttpMessageConverters converters =
				HttpMessageConverters.forClient().registerDefaults().build();

		EventProcessor<Object> eventProcessor = event -> {
			rawEvents.add(event.id() + "|" + event.eventType() + "|" + event.data());
			if ("person".equals(event.eventType())) {
				event.setTargetType(Person.class);
			}
			else if ("people".equals(event.eventType())) {
				event.setTargetType(new ParameterizedTypeReference<List<Person>>() {}, MediaType.APPLICATION_JSON);
			}
		};

		try (Source<ServerSentEvent<Object>> source = performRequest("/sse-typed",
				(url, body) -> new ServerSentEventSource<>(url, body, converters, eventProcessor))) {

			List<ServerSentEvent<Object>> events = new ArrayList<>();
			while (source.receiveNext()) {
				events.add(source.next());
			}
			assertThat(events)
					.extracting(ServerSentEvent::id, ServerSentEvent::event, ServerSentEvent::data)
					.containsExactly(
							tuple("1", "person", new Person("Joe", 42)),
							tuple("2", "people", List.of(new Person("Jane", 35), new Person("Jim", 27))),
							tuple("3", "message", "plain text"));
		}

		assertThat(rawEvents).containsExactly(
				"1|person|{\"name\":\"Joe\",\"age\":42}",
				"2|people|[{\"name\":\"Jane\",\"age\":35},{\"name\":\"Jim\",\"age\":27}]",
				"3|message|plain text");
	}


	private static Source<ServerSentEvent<String>> performRequest(String path) {
		return performRequest(path, ServerSentEventSource::new);
	}

	private static <T> Source<ServerSentEvent<T>> performRequest(
			String path, BiFunction<URI, InputStream, ServerSentEventSource<T>> sourceFactory) {

		return restClient.get().uri(path).exchangeForRequiredValue((request, response) -> {
			if (response.getStatusCode().isError()) {
				throw response.createException();
			}
			ServerSentEventSource<T> source = sourceFactory.apply(request.getURI(), response.getBody());
			return ActiveSource.fromSource(source).start();
		}, false);
	}


	public record Person(String name, int age) {
	}


	@EnableWebMvc
	@Configuration
	public static class WebConfig implements WebMvcConfigurer {

		@Bean
		public SseController sseController() {
			return new SseController();
		}

		@Override
		public void configureAsyncSupport(AsyncSupportConfigurer configurer) {
			configurer.setTaskExecutor(new VirtualThreadTaskExecutor("spring-mvc-"));
		}
	}


	@RestController
	public static class SseController {

		private static final Logger logger = LoggerFactory.getLogger(SseController.class);

		private static final Duration SSE_EVENT_GAP = Duration.ofMillis(300);

		private static final Duration EMPTY_COMPLETION_DELAY = Duration.ofMillis(100);


		@GetMapping("/sse")
		public SseEmitter sse() {
			SseEmitter emitter = new SseEmitter();
			Thread.ofVirtual().start(() -> {
				try {
					emitter.send(SseEmitter.event().id("1").data("data-1"));
					Thread.sleep(SSE_EVENT_GAP);
					emitter.send(SseEmitter.event().id("2").data("data-2"));
					emitter.complete();
				}
				catch (Exception ex) {
					logger.error("Failed to emit SSE events", ex);
				}
			});
			return emitter;
		}

		@GetMapping("/sse-typed")
		public SseEmitter sseTyped() {
			SseEmitter emitter = new SseEmitter();
			Thread.ofVirtual().start(() -> {
				try {
					emitter.send(SseEmitter.event().id("1").name("person")
							.data(new Person("Joe", 42), MediaType.APPLICATION_JSON));
					emitter.send(SseEmitter.event().id("2").name("people")
							.data(List.of(new Person("Jane", 35), new Person("Jim", 27)), MediaType.APPLICATION_JSON));
					emitter.send(SseEmitter.event().id("3").name("message").data("plain text"));
					emitter.complete();
				}
				catch (Exception ex) {
					logger.error("Failed to emit SSE events", ex);
				}
			});
			return emitter;
		}

		@GetMapping("/sse-complete-empty")
		public SseEmitter sseEmpty() {
			SseEmitter emitter = new SseEmitter();
			Thread.ofVirtual().start(() -> {
				try {
					Thread.sleep(EMPTY_COMPLETION_DELAY);
					emitter.complete();
				}
				catch (Exception ex) {
					logger.error("Failed to emit SSE events", ex);
				}
			});
			return emitter;
		}

		@GetMapping("/sse-complete-with-error")
		public SseEmitter sseError() {
			SseEmitter emitter = new SseEmitter();
			Thread.ofVirtual().start(() -> {
				try {
					emitter.send(SseEmitter.event().id("1").data("data-1"));
					Thread.sleep(SSE_EVENT_GAP);
					emitter.completeWithError(new Exception("simulated error"));
				}
				catch (Exception ex) {
					logger.error("Failed to emit SSE events", ex);
				}
			});
			return emitter;
		}

	}

}
