package http.server;

/**
 * A reusable, embeddable HTTP server for integration tests.
 */
public interface HttpServer {

	/**
	 * Register configuration classes (e.g. {@code @Configuration} classes) to
	 * set up the web application context served by this server. Must be
	 * called before {@link #start()}.
	 * @param configClasses the configuration classes to register
	 */
	void register(Class<?>... configClasses);

	/**
	 * Start the server on a local port.
	 */
	void start() throws Exception;

	/**
	 * Stop the server and release its resources.
	 */
	void stop() throws Exception;

	/**
	 * The local port the server is listening on.
	 */
	int getPort();

	/**
	 * The base URL of the server, e.g. {@code "http://localhost:12345"}.
	 */
	default String getBaseUrl() {
		return "http://localhost:" + getPort();
	}

}
