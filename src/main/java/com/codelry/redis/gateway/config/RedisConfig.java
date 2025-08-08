package com.codelry.redis.gateway.config;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SslOptions;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.resource.ClientResources;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.convert.DurationUnit;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettucePoolingClientConfiguration;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.util.StringUtils;
import org.springframework.context.annotation.DependsOn;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Configuration
public class RedisConfig {

    private static final Logger logger = LoggerFactory.getLogger(RedisConfig.class);

    @Value("${spring.data.redis.host:localhost}")
    private String redisHost;

    @Value("${spring.data.redis.port:6379}")
    private int redisPort;

    @Value("${spring.data.redis.password:}")
    private String redisPassword;

    @Value("${spring.data.redis.database:0}")
    private int redisDatabase;

    @Value("${spring.data.redis.ssl.enabled:false}")
    private boolean useSsl;

    @Value("${spring.data.redis.ssl.ssl-verify:true}")
    private boolean sslVerify;

    @Value("${spring.data.redis.ssl.keystore.path:}")
    private String keystorePath;

    @Value("${spring.data.redis.ssl.keystore.password:}")
    private String keystorePassword;

    @Value("${spring.data.redis.ssl.keystore.type:PKCS12}")
    private String keystoreType;

    @Value("${spring.data.redis.ssl.truststore.path:}")
    private String truststorePath;

    @Value("${spring.data.redis.ssl.truststore.password:}")
    private String truststorePassword;

    @Value("${spring.data.redis.ssl.truststore.type:PKCS12}")
    private String truststoreType;

    @Value("${spring.data.redis.timeout:5000ms}")
    @DurationUnit(ChronoUnit.MILLIS)
    private Duration timeout;

    @Value("${spring.data.redis.lettuce.pool.max-active:20}")
    private int maxActive;

    @Value("${spring.data.redis.lettuce.pool.max-idle:10}")
    private int maxIdle;

    @Value("${spring.data.redis.lettuce.pool.min-idle:2}")
    private int minIdle;

    @Value("${spring.data.redis.lettuce.pool.max-wait:5000ms}")
    @DurationUnit(ChronoUnit.MILLIS)
    private Duration maxWait;

    @Bean
    public GenericObjectPoolConfig<StatefulConnection<?, ?>> redisPoolConfig() {
      GenericObjectPoolConfig<StatefulConnection<?, ?>> poolConfig = new GenericObjectPoolConfig<>();
      poolConfig.setMaxTotal(maxActive);
      poolConfig.setMaxIdle(maxIdle);
      poolConfig.setMinIdle(minIdle);
      poolConfig.setMaxWait(maxWait);
      poolConfig.setTestOnBorrow(true);
      poolConfig.setTestOnReturn(true);
      poolConfig.setTestWhileIdle(true);
      return poolConfig;
    }

    @Bean
    public RedisConnectionFactory redisConnectionFactory(ClientResources clientResources) throws Exception {
      RedisStandaloneConfiguration config = new RedisStandaloneConfiguration();
      config.setHostName(redisHost);
      config.setPort(redisPort);
      config.setDatabase(redisDatabase);
      logger.info("Redis host: {}, port: {}, database: {}", redisHost, redisPort, redisDatabase);

      if (StringUtils.hasText(redisPassword)) {
        config.setPassword(redisPassword);
        logger.info("Redis password authentication enabled");
      }

      LettuceClientConfiguration.LettuceClientConfigurationBuilder clientConfigBuilder =
          LettucePoolingClientConfiguration.builder()
              .commandTimeout(timeout)
              .poolConfig(redisPoolConfig())
              .clientResources(clientResources);

      if (useSsl) {
        logger.info("Configuring Redis connection with SSL");
        SslOptions sslOptions = createSslOptions();
        clientConfigBuilder.useSsl().and().clientOptions(ClientOptions.builder()
            .autoReconnect(true)
            .pingBeforeActivateConnection(true)
            .sslOptions(sslOptions)
            .build());
      }

      LettuceClientConfiguration clientConfig = clientConfigBuilder.build();

      return new LettuceConnectionFactory(config, clientConfig);
    }

    @Bean
    public RedisTemplate<String, String> redisTemplate(RedisConnectionFactory connectionFactory) {
        RedisTemplate<String, String> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
        template.setKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(new StringRedisSerializer());
        template.setHashKeySerializer(new StringRedisSerializer());
        template.setHashValueSerializer(new StringRedisSerializer());
        return template;
    }

    @Bean(destroyMethod = "close")
    @DependsOn("redisConnectionFactory")
    public StatefulRedisModulesConnection<String, String> modulesConnection() throws Exception {
      RedisURI.Builder builder = RedisURI.builder()
          .withHost(redisHost)
          .withPort(redisPort)
          .withDatabase(redisDatabase)
          .withSsl(useSsl)
          .withVerifyPeer(sslVerify)
          .withTimeout(Duration.ofSeconds(10));

      if (redisPassword != null && !redisPassword.isEmpty()) {
        builder.withPassword(redisPassword.toCharArray());
      }

      RedisModulesClient client = RedisModulesClient.create(builder.build());

      ClientOptions.Builder options = ClientOptions.builder()
          .autoReconnect(true)
          .pingBeforeActivateConnection(true);

      if (useSsl) {
        SslOptions sslOptions = createSslOptions();
        options.sslOptions(sslOptions);
      }

      client.setOptions(options.build());
      return client.connect();
    }

    private SslOptions createSslOptions() throws Exception {
      SslOptions.Builder sslOptionsBuilder = SslOptions.builder();

      if (StringUtils.hasText(keystorePath)) {
        KeyStore keyStore = KeyStore.getInstance(keystoreType);
        try (FileInputStream fis = new FileInputStream(keystorePath)) {
          keyStore.load(fis, keystorePassword.toCharArray());
        }

        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, keystorePassword.toCharArray());

        sslOptionsBuilder.keyManager(keyManagerFactory);
        logger.info("Client keystore configured: {}", keystorePath);
      }

      if (!sslVerify) {
        sslOptionsBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE);
        logger.info("SSL certificate validation disabled");
      } else if (StringUtils.hasText(truststorePath)) {
        KeyStore trustStore = KeyStore.getInstance(truststoreType);
        try (FileInputStream fis = new FileInputStream(truststorePath)) {
          trustStore.load(fis, truststorePassword.toCharArray());
        }

        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStore);

        sslOptionsBuilder.trustManager(trustManagerFactory);
        logger.info("Client truststore configured: {}", truststorePath);
      } else {
        logger.warn("SSL certificate validation enabled but no truststore configured");
      }

      sslOptionsBuilder.jdkSslProvider();

      return sslOptionsBuilder.build();
    }
}
