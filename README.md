## Overview

Experimental support for the use of the [stream-source](https://github.com/spring-projects-experimental/stream-source) library in Spring projects.

## SSE Streaming for RestClient

[ServerSentEventSource](src/main/java/org/springframework/experimental/streamsource/web/client/support/ServerSentEventSource.java) is an implementation of [Source](https://github.com/spring-projects-experimental/stream-source/blob/main/DESIGN.md#source) that parses and decodes Server-sent events.

For events with String data (not converted):
```java
InputStream body = ...;
ServerSentEventSource<String> source = new ServerSentEventSource<>(uri, body);
```

For events with data converted to higher level objects, also provide `HttpMessageConverters` and an `EventProcessor` to set the target Java type, and optionally the MediaType to choose an `HttpMessageConverter`:

```java
ServerSentEventSource<Person> source =
        new ServerSentEventSource<Person>(url, body, converters, event -> {
            event.setTargetType(Person.class);
        });
```

For example use with `RestClient`, see [SseIntegrationTests](src/test/java/org/springframework/experimental/streamsource/web/client/support/SseIntegrationTests.java).

## Maven Artefact

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
