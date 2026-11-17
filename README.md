
Experimental support for the use of [stream-source](https://github.com/spring-projects-experimental/stream-source) in Spring projects.

Currently, the project provides an SSE `Source` that decodes an SSE stream via `HttpMessageConverters`. 

To convert the data of each parsed event, pass `HttpMessageConverters` and an `EventProcessor`.
The processor determines the target Java type and optionally a media type to select a converter.
If the target type is not set, the raw String data is used.
For example:

```java
new ServerSentEventSource<Object>(url, body, converters, event -> {
	event.setTargetType(Person.class);
});
```

For use with `RestClient`, see [SseIntegrationTests](src/test/java/org/springframework/experimental/streamsource/web/client/support/SseIntegrationTests.java).


**Maven coordinates:**
```xml
<dependency>
    <groupId>org.springframework.experimental.streamsource</groupId>
    <artifactId>stream-source-spring-extensions</artifactId>
    <version>0.8.0-SNAPSHOT</version>
</dependency>
```

## Build

Currently `stream-source` is resolved from the local Maven repository.
To install it, clone [spring-projects-experimental/stream-source](https://github.com/spring-projects-experimental/stream-source) locally and run `./gradlew publishToMavenLocal`.
