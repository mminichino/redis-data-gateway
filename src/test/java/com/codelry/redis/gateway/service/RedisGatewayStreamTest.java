package com.codelry.redis.gateway.service;

import com.codelry.redis.gateway.data.*;
import com.codelry.redis.gateway.stream.JsonItem;
import com.codelry.redis.gateway.stream.RedisStreamerGrpc;
import com.codelry.redis.gateway.stream.StreamRequest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.testcontainers.RedisContainer;
import net.devh.boot.grpc.client.channelfactory.GrpcChannelConfigurer;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import javax.net.ssl.SSLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(
    properties = {
        "grpc.server.port=9443",
        "logging.level.com.codelry=DEBUG"
    }
)
@Testcontainers
public class RedisGatewayStreamTest {

  private static final Logger logger = LoggerFactory.getLogger(RedisGatewayStreamTest.class);
  private static final Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(logger);

  @Container
  static GenericContainer<?> redis = new RedisContainer(DockerImageName.parse("redis/redis-stack:latest"))
      .waitingFor(Wait.forListeningPort());

  @GrpcClient("GLOBAL")
  private RedisGatewayGrpc.RedisGatewayBlockingStub blockingStub;

  @GrpcClient("GLOBAL")
  private RedisStreamerGrpc.RedisStreamerBlockingStub streamerBlockingStub;

  @DynamicPropertySource
  static void configureProperties(DynamicPropertyRegistry registry) {
    registry.add("spring.data.redis.host", redis::getHost);
    registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379).toString());
    registry.add("spring.data.redis.password", () -> "");
    registry.add("spring.data.redis.database", () -> "0");
    registry.add("spring.data.redis.ssl.enabled", () -> "false");
    registry.add("grpc.client.GLOBAL.address", () -> "static://localhost:9443");
    registry.add("grpc.client.GLOBAL.negotiationType", () -> "TLS");
  }

  @TestConfiguration
  static class InsecureTlsClientConfig {
    @Bean
    GrpcChannelConfigurer insecureTrustAllConfigurer() {
      return (builder, name) -> {
        if (!"GLOBAL".equals(name)) {
          return;
        }
        if (builder instanceof io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder netty) {
          try {
            var sslContext = io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts.forClient()
                .trustManager(io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory.INSTANCE)
                .build();
            netty.sslContext(sslContext).useTransportSecurity();
          } catch (SSLException e) {
            throw new IllegalStateException("Failed to configure test TLS trust-all", e);
          }
        }
      };
    }
  }

  @BeforeAll
  public static void beforeAll() {
    redis.followOutput(logConsumer);
  }

  @Test
  public void testStreamOperation() {
    long timestamp = System.currentTimeMillis();
    String key1 = "test:stream:1:" + timestamp;
    String key2 = "test:stream:2:" + timestamp;
    String key3 = "test:stream:3:" + timestamp;
    String key4 = "test:stream:4:" + timestamp;
    String key5 = "test:stream:5:" + timestamp;

    PutResponse r1 = blockingStub.put(PutRequest.newBuilder().setKey(key1).setValue("value1").build());
    PutResponse r2 = blockingStub.put(PutRequest.newBuilder().setKey(key2).setValue("value2").build());
    PutResponse r3 = blockingStub.put(PutRequest.newBuilder().setKey(key3).setValue("value3").build());
    PutResponse r4 = blockingStub.put(PutRequest.newBuilder().setKey(key4).setValue("value4").build());
    PutResponse r5 = blockingStub.put(PutRequest.newBuilder().setKey(key5).setValue("value5").build());
    assertTrue(r1.getSuccess() && r2.getSuccess() && r3.getSuccess() && r4.getSuccess() && r5.getSuccess(),
        "All puts should be successful");

    String pattern = "test:stream:*:" + timestamp;
    StreamRequest request = StreamRequest.newBuilder()
        .setPattern(pattern)
        .setCount(100)
        .build();

    Iterator<JsonItem> iterator = assertDoesNotThrow(
        () -> streamerBlockingStub.streamByPattern(request),
        "Obtaining server-streaming iterator should not throw"
    );

    AtomicInteger count = new AtomicInteger();
    Set<String> receivedKeys = new HashSet<>();
    ObjectMapper mapper = new ObjectMapper();

    assertDoesNotThrow(() -> {
      while (iterator.hasNext()) {
        JsonItem item = iterator.next();
        String json = item.getJson().toStringUtf8();
        JsonNode node = mapper.readTree(json);
        node.fieldNames().forEachRemaining(receivedKeys::add);
        count.getAndIncrement();
      }
    }, "Consuming the server stream should not throw");

    assertEquals(5, count.get(), "Expected 5 streamed results");
    assertTrue(receivedKeys.containsAll(Set.of(key1, key2, key3, key4, key5)),
        "Stream should contain all inserted keys");
    logger.info("testStreamOperation: SUCCESS: Received {} items", count.get());
  }
}
