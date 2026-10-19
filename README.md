
Experimental support for use of [stream-source](https://github.com/spring-projects-experimental/stream-source) in Spring projects.

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

`stream-source` is currently resolved from the local Maven repository.
Install it first, by running `./gradlew publishToMavenLocal` in the `stream-source` repository, then build this project with `./gradlew build`.
