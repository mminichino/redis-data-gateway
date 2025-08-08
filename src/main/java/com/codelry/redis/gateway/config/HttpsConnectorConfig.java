package com.codelry.redis.gateway.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.server.Ssl;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Files;
import java.nio.file.Path;

@Configuration
public class HttpsConnectorConfig {

  private final RedisKeystoreService keystoreService;
  private final char[] keystorePassword;
  private final Integer httpsPort;

  public HttpsConnectorConfig(
      RedisKeystoreService keystoreService,
      @Value("${app.ssl.keystore.password:password}") String keystorePassword,
      @Value("${server.https.port:8443}") Integer httpsPort
  ) {
    this.keystoreService = keystoreService;
    this.keystorePassword = keystorePassword.toCharArray();
    this.httpsPort = httpsPort;
  }

  @Bean
  public WebServerFactoryCustomizer<TomcatServletWebServerFactory> sslCustomizer() {
    return factory -> {
      try {
        byte[] keystoreBytes = keystoreService.ensureAndGetKeystoreBytes();

        Path tempKs = Files.createTempFile("gateway-https-", ".p12");
        Files.write(tempKs, keystoreBytes);
        tempKs.toFile().deleteOnExit();

        Ssl ssl = new Ssl();
        ssl.setEnabled(true);
        ssl.setKeyAlias(RedisKeystoreService.KEY_ALIAS);
        ssl.setKeyStoreType(RedisKeystoreService.KEYSTORE_TYPE);
        ssl.setKeyStorePassword(new String(keystorePassword));
        ssl.setKeyStore(tempKs.toUri().toString());

        factory.setSsl(ssl);
        factory.setPort(httpsPort != null ? httpsPort : 8443);
      } catch (Exception e) {
        throw new IllegalStateException("Failed to configure HTTPS from Redis keystore", e);
      }
    };
  }
}
