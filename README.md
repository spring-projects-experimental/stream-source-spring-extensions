
Experimental support for the use of [stream-source](https://github.com/spring-projects-experimental/stream-source) in Spring projects.

Currently, the project provides an SSE `Source` that decodes an SSE stream and uses HTTP messages converters to convert event data to a target object type. 

For use with `RestClient` or `RestTemplate`, see [SseIntegrationTests](src/test/java/org/springframework/experimental/streamsource/web/client/support/SseIntegrationTests.java) for usage.


**Maven coordinates:**
```xml
<dependency>
    <groupId>org.springframework.experimental.streamsource</groupId>
    <artifactId>stream-source-spring-extensions</artifactId>
    <version>0.0.1-SNAPSHOT</version>
</dependency>
```

## Build

Currently `stream-source` is resolved from the local Maven repository.
To install it, clone [spring-projects-experimental/stream-source](https://github.com/spring-projects-experimental/stream-source) locally and run `./gradlew publishToMavenLocal`.
