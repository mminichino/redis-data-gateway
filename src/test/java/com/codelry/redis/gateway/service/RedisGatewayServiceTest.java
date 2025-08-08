package com.codelry.redis.gateway.service;

import com.codelry.redis.gateway.data.*;
import com.redis.testcontainers.RedisContainer;
import net.devh.boot.grpc.client.channelfactory.GrpcChannelConfigurer;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.containers.wait.strategy.Wait;

import javax.net.ssl.SSLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(
    properties = {
        "grpc.server.port=9443",
        "logging.level.com.codelry=DEBUG"
    }
)
@Testcontainers
public class RedisGatewayServiceTest {

  @Container
  static GenericContainer<?> redis = new RedisContainer(DockerImageName.parse("redis/redis-stack:latest"))
      .waitingFor(Wait.forListeningPort());

  @GrpcClient("GLOBAL")
  private RedisGatewayGrpc.RedisGatewayBlockingStub blockingStub;

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

  @Test
  public void testStringDataType() {
    String key = "test:string:" + System.currentTimeMillis();
    String value = "Hello, Redis!";

    PutRequest putRequest = PutRequest.newBuilder()
        .setKey(key)
        .setValue(value)
        .build();
    PutResponse putResponse = blockingStub.put(putRequest);
    assertTrue(putResponse.getSuccess(), "String put should be successful");

    GetRequest getRequest = GetRequest.newBuilder()
        .setKey(key)
        .build();
    GetResponse getResponse = blockingStub.get(getRequest);
    assertEquals(value, getResponse.getValue(), "Retrieved string should match");
  }

  @Test
  public void testHashDataType() {
    String key = "test:hash:" + System.currentTimeMillis();
    String field = "name";
    String value = "John Doe";

    HSetRequest hsetRequest = HSetRequest.newBuilder()
        .setKey(key)
        .setField(field)
        .setValue(value)
        .build();
    PutResponse hsetResponse = blockingStub.hSet(hsetRequest);
    assertTrue(hsetResponse.getSuccess(), "Hash set should be successful");

    HGetRequest hgetRequest = HGetRequest.newBuilder()
        .setKey(key)
        .setField(field)
        .build();
    GetResponse hgetResponse = blockingStub.hGet(hgetRequest);
    assertEquals(value, hgetResponse.getValue(), "Retrieved hash field should match");

    HGetAllRequest hgetAllRequest = HGetAllRequest.newBuilder()
        .setKey(key)
        .build();
    HGetAllResponse hgetAllResponse = blockingStub.hGetAll(hgetAllRequest);
    Map<String, String> fields = hgetAllResponse.getFieldsMap();
    assertTrue(fields.containsKey(field), "Hash should contain the field");
    assertEquals(value, fields.get(field), "Hash field value should match");
  }

  @Test
  public void testSetDataType() {
    String key = "test:set:" + System.currentTimeMillis();
    List<String> members = Arrays.asList("member1", "member2", "member3");

    SAddRequest saddRequest = SAddRequest.newBuilder()
        .setKey(key)
        .addAllMembers(members)
        .build();
    PutResponse saddResponse = blockingStub.sAdd(saddRequest);
    assertTrue(saddResponse.getSuccess(), "Set add should be successful");

    SMembersRequest smembersRequest = SMembersRequest.newBuilder()
        .setKey(key)
        .build();
    SMembersResponse smembersResponse = blockingStub.sMembers(smembersRequest);
    List<String> retrievedMembers = smembersResponse.getMembersList();

    assertEquals(members.size(), retrievedMembers.size(), "Set should contain all members");
    for (String member : members) {
      assertTrue(retrievedMembers.contains(member), "Set should contain " + member);
    }
  }

  @Test
  public void testSortedSetDataType() {
    String key = "test:sortedset:" + System.currentTimeMillis();

    ScoreMember member1 = ScoreMember.newBuilder()
        .setScore(10.5)
        .setMember("player1")
        .build();
    ScoreMember member2 = ScoreMember.newBuilder()
        .setScore(20.0)
        .setMember("player2")
        .build();
    ScoreMember member3 = ScoreMember.newBuilder()
        .setScore(15.7)
        .setMember("player3")
        .build();

    ZAddRequest zaddRequest = ZAddRequest.newBuilder()
        .setKey(key)
        .addAllMembers(Arrays.asList(member1, member2, member3))
        .build();
    PutResponse zaddResponse = blockingStub.zAdd(zaddRequest);
    assertTrue(zaddResponse.getSuccess(), "Sorted set add should be successful");

    ZRangeRequest zrangeRequest = ZRangeRequest.newBuilder()
        .setKey(key)
        .setStart(0)
        .setStop(-1)
        .build();
    ZRangeResponse zrangeResponse = blockingStub.zRange(zrangeRequest);
    List<String> rangeMembers = zrangeResponse.getMembersList();

    assertFalse(rangeMembers.isEmpty(), "Sorted set should not be empty");
    assertEquals(3, rangeMembers.size(), "Sorted set should contain all 3 members");
    assertEquals("player1", rangeMembers.get(0), "First member should be player1 (lowest score)");
    assertEquals("player3", rangeMembers.get(1), "Second member should be player3");
    assertEquals("player2", rangeMembers.get(2), "Third member should be player2 (highest score)");
  }

  @Test
  public void testVectorDataType() {
    String key = "test:vector:" + System.currentTimeMillis();
    String field = "embedding";
    List<Float> vector = Arrays.asList(0.1f, 0.2f, 0.3f, 0.4f, 0.5f);
    Map<String, String> metadata = new HashMap<>();
    metadata.put("type", "image");
    metadata.put("category", "animal");

    VectorAddRequest vectorAddRequest = VectorAddRequest.newBuilder()
        .setKey(key)
        .setField(field)
        .addAllVector(vector)
        .putAllMetadata(metadata)
        .build();
    PutResponse vectorAddResponse = blockingStub.vectorAdd(vectorAddRequest);
    assertTrue(vectorAddResponse.getSuccess(), "Vector add should be successful");
  }

  @Test
  public void testJsonDataType() {
    String key = "test:json:" + System.currentTimeMillis();
    String path = "$";
    String jsonValue = "{\"name\":\"John\",\"age\":30,\"city\":\"New York\",\"active\":true}";

    JsonSetRequest jsonSetRequest = JsonSetRequest.newBuilder()
        .setKey(key)
        .setPath(path)
        .setJson(jsonValue)
        .build();
    PutResponse jsonSetResponse = blockingStub.jsonSet(jsonSetRequest);
    assertTrue(jsonSetResponse.getSuccess(), "JSON set should be successful");

    JsonGetRequest jsonGetRequest = JsonGetRequest.newBuilder()
        .setKey(key)
        .setPath(path)
        .build();
    JsonGetResponse jsonResponse = blockingStub.jsonGet(jsonGetRequest);
    assertNotNull(jsonResponse.getJson(), "JSON get response should not be null");
    assertFalse(jsonResponse.getJson().isEmpty(), "JSON response should not be empty");

    String responseJson = jsonResponse.getJson();
    assertTrue(responseJson.contains("John") || responseJson.contains("\"John\""),
        "JSON should contain expected name");
    assertTrue(responseJson.contains("30") || responseJson.contains("\"30\""),
        "JSON should contain expected age");
    assertTrue(responseJson.contains("New York") || responseJson.contains("\"New York\""),
        "JSON should contain expected city");
  }

  @Test
  public void testDeleteOperation() {
    String key = "test:delete:" + System.currentTimeMillis();
    String value = "to be deleted";

    PutRequest putRequest = PutRequest.newBuilder()
        .setKey(key)
        .setValue(value)
        .build();
    PutResponse putResponse = blockingStub.put(putRequest);
    assertTrue(putResponse.getSuccess(), "Put should be successful");

    GetRequest getRequest = GetRequest.newBuilder()
        .setKey(key)
        .build();
    GetResponse getResponse = blockingStub.get(getRequest);
    assertEquals(value, getResponse.getValue(), "Value should exist before deletion");

    DeleteRequest deleteRequest = DeleteRequest.newBuilder()
        .addKeys(key)
        .build();
    DeleteResponse deleteResponse = blockingStub.delete(deleteRequest);
    assertTrue(deleteResponse.getSuccess(), "Delete should be successful");
    assertEquals(1, deleteResponse.getDeletedCount(), "Should delete exactly one key");

    GetResponse getResponseAfterDelete = blockingStub.get(getRequest);
    assertTrue(getResponseAfterDelete.getValue().isEmpty(), "Value should be empty after deletion");
  }

  @Test
  public void testCompleteWorkflow() {
    long timestamp = System.currentTimeMillis();
    String baseKey = "test:workflow:" + timestamp;

    PutResponse stringPutResponse = blockingStub.put(PutRequest.newBuilder()
        .setKey(baseKey + ":string")
        .setValue("workflow test")
        .build());
    assertTrue(stringPutResponse.getSuccess(), "String put should succeed");

    PutResponse hashPutResponse = blockingStub.hSet(HSetRequest.newBuilder()
        .setKey(baseKey + ":hash")
        .setField("status")
        .setValue("active")
        .build());
    assertTrue(hashPutResponse.getSuccess(), "Hash set should succeed");

    PutResponse setPutResponse = blockingStub.sAdd(SAddRequest.newBuilder()
        .setKey(baseKey + ":set")
        .addMembers("item1")
        .addMembers("item2")
        .build());
    assertTrue(setPutResponse.getSuccess(), "Set add should succeed");

    PutResponse jsonPutResponse = blockingStub.jsonSet(JsonSetRequest.newBuilder()
        .setKey(baseKey + ":json")
        .setPath("$")
        .setJson("{\"workflow\":\"complete\"}")
        .build());
    assertTrue(jsonPutResponse.getSuccess(), "JSON set should succeed");

    GetResponse stringResponse = blockingStub.get(GetRequest.newBuilder()
        .setKey(baseKey + ":string")
        .build());
    assertEquals("workflow test", stringResponse.getValue(), "String value should match");

    GetResponse hashResponse = blockingStub.hGet(HGetRequest.newBuilder()
        .setKey(baseKey + ":hash")
        .setField("status")
        .build());
    assertEquals("active", hashResponse.getValue(), "Hash field value should match");

    SMembersResponse setResponse = blockingStub.sMembers(SMembersRequest.newBuilder()
        .setKey(baseKey + ":set")
        .build());
    assertTrue(setResponse.getMembersList().contains("item1"), "Set should contain item1");
    assertTrue(setResponse.getMembersList().contains("item2"), "Set should contain item2");

    JsonGetResponse jsonResponse = blockingStub.jsonGet(JsonGetRequest.newBuilder()
        .setKey(baseKey + ":json")
        .setPath("$")
        .build());
    assertNotNull(jsonResponse.getJson(), "JSON response should not be null");
    assertTrue(jsonResponse.getJson().contains("complete"), "JSON should contain expected data");
  }

  @Test
  public void testMultipleDeleteOperation() {
    long timestamp = System.currentTimeMillis();
    String key1 = "test:multi-delete:1:" + timestamp;
    String key2 = "test:multi-delete:2:" + timestamp;
    String key3 = "test:multi-delete:3:" + timestamp;

    PutResponse r1 = blockingStub.put(PutRequest.newBuilder().setKey(key1).setValue("value1").build());
    PutResponse r2 = blockingStub.put(PutRequest.newBuilder().setKey(key2).setValue("value2").build());
    PutResponse r3 = blockingStub.put(PutRequest.newBuilder().setKey(key3).setValue("value3").build());
    assertTrue(r1.getSuccess() && r2.getSuccess() && r3.getSuccess(), "All puts should be successful");

    DeleteRequest deleteRequest = DeleteRequest.newBuilder()
        .addKeys(key1)
        .addKeys(key2)
        .addKeys(key3)
        .build();
    DeleteResponse deleteResponse = blockingStub.delete(deleteRequest);

    assertTrue(deleteResponse.getSuccess(), "Multi-delete should be successful");
    assertEquals(3, deleteResponse.getDeletedCount(), "Should delete exactly 3 keys");
  }
}
