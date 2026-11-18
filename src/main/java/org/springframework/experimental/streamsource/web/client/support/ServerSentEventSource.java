package org.springframework.experimental.streamsource.web.client.support;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import io.spring.streamsource.AbstractSource;
import org.jspecify.annotations.Nullable;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.ResolvableType;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.http.converter.GenericHttpMessageConverter;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.HttpMessageConverters;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.SmartHttpMessageConverter;
import org.springframework.util.ObjectUtils;
import org.springframework.web.client.RestClientException;


/**
 * {@link io.spring.streamsource.Source} that parses a Server-Sent Events stream
 * and produces a {@link ServerSentEvent} for each received event.
 *
 * <p>An {@link EventProcessor} delegate helps to resolve the target Java type
 * and media type for each parsed event. For example:
 *
 * <pre class="code">
 * new ServerSentEventSource&lt;Object&gt;(url, body, converters, event -&gt; {
 *     event.setTargetType(Person.class);
 * });
 * </pre>
 *
 * @param <T> the type of event data
 */
public class ServerSentEventSource<T> extends AbstractSource<ServerSentEvent<T>> {

	private final URI uri;

	private final BufferedReader reader;

	private final HttpMessageConverters converters;

	private final EventProcessor<T> eventProcessor;

	private boolean firstLine = true;


	/**
	 * Create an instance whose event data is the raw String.
	 * @param uri the URL of the request, for logging purposes
	 * @param inputStream the response body to read from
	 */
	public ServerSentEventSource(URI uri, InputStream inputStream) {
		this(uri, inputStream, HttpMessageConverters.forClient().build(), parsedEvent -> {});
	}

	/**
	 * Create an instance that converts event data to a higher level Java object.
	 * @param uri the URL of the request, for logging purposes
	 * @param responseBody the response body to read from
	 * @param converters the converters to convert raw event data with
	 * @param processor a processor to determine the target Java type and/or MediaType of parsed events
	 */
	public ServerSentEventSource(
			URI uri, InputStream responseBody,
			HttpMessageConverters converters, EventProcessor<T> processor) {

		this.uri = uri;
		this.reader = new BufferedReader(new InputStreamReader(responseBody, StandardCharsets.UTF_8));
		this.converters = converters;
		this.eventProcessor = processor;
	}


	@Override
	protected final @Nullable ServerSentEvent<T> receiveItem() throws IOException {

		DefaultParsedEvent event = new DefaultParsedEvent();

		while (true) {
			String line = this.reader.readLine();
			if (line == null) {
				// WHATWG: discard an incomplete event at the end of the stream
				return null;
			}

			// WHATWG: ignore and strip a single leading UTF-8 Byte Order Mark (BOM)
			if (this.firstLine) {
				this.firstLine = false;
				if (line.startsWith("\uFEFF")) {
					line = line.substring(1);
				}
			}

			// Only an empty line ends an event, not a whitespace-only line
			if (!line.isEmpty()) {
				event.addField(line);
				continue;
			}

			// End of event

			// WHATWG: don't dispatch an event without data, and start over
			if (!event.hasData()) {
				event = new DefaultParsedEvent();
				continue;
			}

			this.eventProcessor.process(event);

			T data = readWithMessageConverters(event);

			ServerSentEvent.Builder<T> builder = ServerSentEvent.builder();
			if (event.id() != null) {
				builder.id(event.id());
			}
			if (event.eventType() != null) {
				builder.event(event.eventType());
			}
			if (event.retry() != null) {
				builder.retry(event.retry());
			}
			if (data != null) {
				builder.data(data);
			}
			return builder.build();
		}
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	private <D> @Nullable D readWithMessageConverters(DefaultParsedEvent event) {

		String data = event.data();
		ResolvableType targetType = event.targetType();
		MediaType mediaType = event.mediaType();

		if (targetType.getRawClass() == String.class) {
			return (D) data;
		}

		if (ObjectUtils.isEmpty(data)) {
			return null;
		}

		HttpInputMessage inputMessage = new ByteArrayHttpInputMessage(mediaType, data);

		try {
			for (HttpMessageConverter<?> converter : this.converters) {
				if (converter instanceof GenericHttpMessageConverter genericConverter) {
					if (genericConverter.canRead(targetType.getType(), null, mediaType)) {
						return (D) genericConverter.read(targetType.getType(), null, inputMessage);
					}
				}
				else if (converter instanceof SmartHttpMessageConverter smartConverter) {
					if (smartConverter.canRead(targetType, mediaType)) {
						return (D) smartConverter.read(targetType, inputMessage, null);
					}
				}
				else {
					if (converter.canRead(targetType.resolve(Object.class), mediaType)) {
						return (D) converter.read((Class) targetType.resolve(Object.class), inputMessage);
					}
				}
			}
			throw new IllegalArgumentException(
					"No HttpMessageConverter for type " + targetType + " and content type " + mediaType);
		}
		catch (UncheckedIOException | IOException | HttpMessageNotReadableException exc) {
			Throwable cause;
			if (exc instanceof UncheckedIOException uncheckedIOException) {
				cause = uncheckedIOException.getCause();
			}
			else {
				cause = exc;
			}
			throw new RestClientException("Error while converting to type [" + targetType + "]", cause);
		}
	}

	@Override
	protected void handleClose(@Nullable Throwable failure) throws IOException {
		this.reader.close();
	}

	@Override
	public String toString() {
		return "ServerSentEventSource[\"" + this.uri + "\"]";
	}


	/**
	 * Processor of parsed events, invoked after an event is parsed and before its
	 * data field is converted to a higher-level object. The processor can choose
	 * the target Java type and/or the MediaType to use for the conversion.
	 * @param <T> the type of event data
	 */
	@FunctionalInterface
	public interface EventProcessor<T> {

		/**
		 * Process the given event, and choose how to convert its data field by
		 * calling one of the {@code setTargetType} methods.
		 * <p>By default, if the target type is not set, the raw String data is used.
		 * @param event the parsed event
		 */
		void process(ParsedEvent<T> event);


		/**
		 * A parsed event whose data has not been converted yet.
		 * @param <T> the type of event data
		 */
		interface ParsedEvent<T> {

			/**
			 * Return the value of the "id" field, if present.
			 */
			@Nullable String id();

			/**
			 * Return the value of the "event" field, if present.
			 */
			@Nullable String eventType();

			/**
			 * Return the value of the "retry" field, if present.
			 */
			@Nullable Duration retry();

			/**
			 * Return the event data, or an empty String if there is no data.
			 */
			String data();

			/**
			 * Set the target Java type to convert the raw event data to, and the
			 * media type to use to select a converter. The converted data becomes
			 * the data of the resulting {@code ServerSentEvent}.
			 * <p>If not called, the data is used as is, as a {@code String}.
			 * @param targetType the type to convert to
			 * @param mediaType the media type of the data, or {@code null} to
			 * select a converter by target type only
			 */
			void setTargetType(Class<? extends T> targetType, @Nullable MediaType mediaType);

			/**
			 * Variant of {@link #setTargetType(Class, MediaType)} without a media
			 * type, selecting a converter by target type only.
			 * @param targetType the type to convert to
			 */
			default void setTargetType(Class<? extends T> targetType) {
				setTargetType(targetType, null);
			}

			/**
			 * Variant of {@link #setTargetType(Class, MediaType)} for a
			 * parameterized target type such as {@code List<Person>}.
			 * @param targetType the type to convert to
			 * @param mediaType the media type of the data, or {@code null} to
			 * select a converter by target type only
			 */
			void setTargetType(ParameterizedTypeReference<? extends T> targetType, @Nullable MediaType mediaType);

			/**
			 * Variant of {@link #setTargetType(ParameterizedTypeReference, MediaType)}
			 * without a media type, selecting a converter by target type only.
			 * @param targetType the type to convert to
			 */
			default void setTargetType(ParameterizedTypeReference<? extends T> targetType) {
				setTargetType(targetType, null);
			}
		}
	}


	/**
	 * {@link EventProcessor.ParsedEvent} that is populated while parsing an event.
	 */
	private final class DefaultParsedEvent implements EventProcessor.ParsedEvent<T> {

		private @Nullable String id;

		private @Nullable String eventType;

		private @Nullable Duration retry;

		private final StringBuilder data = new StringBuilder();

		private boolean hasData;

		private ResolvableType targetType = ResolvableType.forClass(String.class);

		private @Nullable MediaType mediaType;

		@Override
		public @Nullable String id() {
			return this.id;
		}

		@Override
		public @Nullable String eventType() {
			return this.eventType;
		}

		@Override
		public @Nullable Duration retry() {
			return this.retry;
		}

		@Override
		public String data() {
			return this.data.toString();
		}

		public ResolvableType targetType() {
			return this.targetType;
		}

		public @Nullable MediaType mediaType() {
			return this.mediaType;
		}

		public boolean hasData() {
			return this.hasData;
		}


		@Override
		public void setTargetType(Class<? extends T> targetType, @Nullable MediaType mediaType) {
			this.targetType = ResolvableType.forClass(targetType);
			this.mediaType = mediaType;
		}

		@Override
		public void setTargetType(ParameterizedTypeReference<? extends T> targetType, @Nullable MediaType mediaType) {
			this.targetType = ResolvableType.forType(targetType);
			this.mediaType = mediaType;
		}

		public void addField(String line) {
			int index = line.indexOf(':');
			if (index == 0) {
				// Comment line
				return;
			}

			String field = (index != -1 ? line.substring(0, index) : line);
			String value = (index != -1 ? line.substring(index + 1) : "");
			if (value.startsWith(" ")) {
				value = value.substring(1);
			}

			switch (field) {
				case "event":
					this.eventType = value;
					break;
				case "data":
					// Multiple data lines are joined with a line feed
					if (this.hasData) {
						this.data.append('\n');
					}
					this.data.append(value);
					this.hasData = true;
					break;
				case "id":
					// Ignore an id with a NULL character
					if (value.indexOf('\u0000') == -1) {
						this.id = value;
					}
					break;
				case "retry":
					this.retry = parseRetry(value);
					break;
			}
		}

		/**
		 * Parse a "retry" value, or keep the current one if the value is not
		 * all ASCII digits, as the spec requires.
		 */
		private @Nullable Duration parseRetry(String value) {
			if (value.isEmpty()) {
				return this.retry;
			}
			for (int i = 0; i < value.length(); i++) {
				char c = value.charAt(i);
				if (c < '0' || c > '9') {
					return this.retry;
				}
			}
			try {
				return Duration.ofMillis(Long.parseLong(value));
			}
			catch (NumberFormatException ex) {
				// Too large
				return this.retry;
			}
		}

		@Override
		public String toString() {
			return "Event[id=" + this.id + ", event=" + this.eventType +
					", retry=" + this.retry + ", data=" + this.data +
					", targetType=" + this.targetType + ", mediaType=" + this.mediaType + "]";
		}
	}


	private static class ByteArrayHttpInputMessage implements HttpInputMessage {

		private final HttpHeaders headers;

		private final InputStream content;

		ByteArrayHttpInputMessage(@Nullable MediaType mediaType, String content) {
			this.headers = new HttpHeaders();
			if (mediaType != null) {
				this.headers.setContentType(mediaType);
			}
			this.content = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
		}

		@Override
		public HttpHeaders getHeaders() {
			return this.headers;
		}

		@Override
		public InputStream getBody() {
			return this.content;
		}
	}

}
