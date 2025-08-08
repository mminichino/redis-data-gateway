package com.codelry.redis.gateway.config;

import org.apache.catalina.connector.Connector;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HttpConnectorConfig {

  @Value("${server.http.enabled:true}")
  private boolean httpEnabled;

  @Value("${server.http.port:8080}")
  private int httpPort;

  @Bean
  public WebServerFactoryCustomizer<TomcatServletWebServerFactory> httpConnectorCustomizer() {
    return factory -> {
      if (!httpEnabled) return;

      Connector httpConnector = new Connector(TomcatServletWebServerFactory.DEFAULT_PROTOCOL);
      httpConnector.setPort(httpPort);
      httpConnector.setScheme("http");
      httpConnector.setSecure(false);

      factory.addAdditionalTomcatConnectors(httpConnector);
    };
  }
}
