package http.server;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.catalina.Context;
import org.apache.catalina.Wrapper;
import org.apache.catalina.startup.Tomcat;

import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.servlet.DispatcherServlet;

/**
 * {@link HttpServer} implementation backed by an embedded {@link Tomcat} running
 * a Spring MVC {@link DispatcherServlet}, bound to a random local port.
 */
public class TomcatHttpServer implements HttpServer {

	private final Tomcat tomcat = new Tomcat();

	private final AnnotationConfigWebApplicationContext webAppContext = new AnnotationConfigWebApplicationContext();

	private Path baseDir;


	@Override
	public void register(Class<?>... configClasses) {
		this.webAppContext.register(configClasses);
	}


	@Override
	public void start() throws Exception {
		this.baseDir = Files.createTempDirectory("tomcat-test-server-");
		this.tomcat.setBaseDir(this.baseDir.toString());
		this.tomcat.setHostname("0.0.0.0");
		this.tomcat.setPort(0);

		DispatcherServlet dispatcherServlet = new DispatcherServlet();
		dispatcherServlet.setApplicationContext(this.webAppContext);

		Context context = this.tomcat.addContext("", null);
		Wrapper wrapper = Tomcat.addServlet(context, "dispatcherServlet", dispatcherServlet);
		wrapper.setLoadOnStartup(1);
		wrapper.setAsyncSupported(true);
		wrapper.addMapping("/");

		this.tomcat.start();
	}

	@Override
	public void stop() throws Exception {
		try {
			this.tomcat.stop();
			this.tomcat.destroy();
		}
		finally {
			deleteRecursively(this.baseDir);
		}
	}

	@Override
	public int getPort() {
		return this.tomcat.getConnector().getLocalPort();
	}

	private static void deleteRecursively(Path path) throws IOException {
		if (path == null || !Files.exists(path)) {
			return;
		}
		try (var files = Files.walk(path)) {
			files.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
				try {
					Files.delete(p);
				}
				catch (IOException ignored) {
				}
			});
		}
	}

}
