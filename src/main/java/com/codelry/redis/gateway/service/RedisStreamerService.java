package com.codelry.redis.gateway.service;

import com.codelry.redis.gateway.stream.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.lettuce.core.*;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import io.lettuce.core.json.JsonPath;
import io.lettuce.core.api.sync.*;
import net.devh.boot.grpc.server.service.GrpcService;
import org.springframework.beans.factory.annotation.Autowired;
import com.redis.lettucemod.api.sync.RedisModulesCommands;

import java.nio.charset.StandardCharsets;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@GrpcService
public class RedisStreamerService extends RedisStreamerGrpc.RedisStreamerImplBase {

  private static final Logger logger = LoggerFactory.getLogger(RedisStreamerService.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String INTERNAL_PREFIX = "__gateway__";
  private final RedisModulesCommands<String, String> modulesCommands;

  @Autowired
  public RedisStreamerService(StatefulRedisModulesConnection<String, String> modulesConnection) {
    this.modulesCommands = modulesConnection.sync();
  }

  @Override
  public void streamByPattern(StreamRequest req, StreamObserver<JsonItem> responseObserver) {
    ServerCallStreamObserver<JsonItem> serverObs =
        (ServerCallStreamObserver<JsonItem>) responseObserver;

    serverObs.disableAutoInboundFlowControl();

    Iterator<JsonItem> itemIterator = scanAsIterator(req);

    serverObs.setOnReadyHandler(() -> {
      try {
        while (serverObs.isReady() && itemIterator.hasNext()) {
          serverObs.onNext(itemIterator.next());
        }
        if (!itemIterator.hasNext()) {
          serverObs.onCompleted();
        }
      } catch (Exception e) {
        logger.error("Error streaming data", e);
        serverObs.onError(e);
      }
    });
  }

  private Iterator<JsonItem> scanAsIterator(StreamRequest req) {
    String pattern = req.getPattern().isBlank() ? "*" : req.getPattern();

    int requested = req.getCount();
    int count = requested <= 0 ? 200 : Math.min(requested, 10_000);

    ScanArgs args = ScanArgs.Builder.matches(pattern).limit(count);

    return new Iterator<>() {
      ScanCursor cursor = ScanCursor.INITIAL;
      java.util.Iterator<String> pageIter = Collections.emptyIterator();
      boolean finished = false;

      @Override
      public boolean hasNext() {
        if (pageIter.hasNext()) return true;
        if (finished) return false;

        KeyScanCursor<String> page;
        try {
          page = modulesCommands.scan(cursor, args);
        } catch (IllegalArgumentException | RedisException ex) {
          logger.error("SCAN failed (pattern='{}', count={})", pattern, count, ex);
          finished = true;
          return false;
        }

        cursor = page;

        List<String> keys = page.getKeys();
        if (!keys.isEmpty()) {
          keys = keys.stream()
              .filter(k -> !k.startsWith(INTERNAL_PREFIX))
              .toList();
        }
        pageIter = keys.iterator();

        if (!pageIter.hasNext()) {
          finished = cursor.isFinished();
          return !finished && hasNext();
        }
        return true;
      }

      @Override
      public JsonItem next() {
        String key = pageIter.next();
        String type = modulesCommands.type(key);

        JsonNode valueNode;
        if (isRedisJson(modulesCommands, key)) {
          valueNode = getJson(modulesCommands, key);
        } else {
          valueNode = switch (type) {
            case "string" -> stringAsJson(modulesCommands, key);
            case "hash" -> hashAsJson(modulesCommands, key);
            case "set" -> setAsJson(modulesCommands, key);
            case "zset" -> zsetAsJson(modulesCommands, key);
            default -> NullNode.getInstance();
          };
        }

        ObjectNode wrapper = MAPPER.createObjectNode();
        wrapper.set(key, valueNode);
        byte[] bytes = wrapper.toString().getBytes(StandardCharsets.UTF_8);
        return JsonItem.newBuilder().setJson(com.google.protobuf.ByteString.copyFrom(bytes)).build();
      }
    };
  }

  private boolean isRedisJson(RedisModulesCommands<String, String> commands, String key) {
    try {
      return commands.jsonType(key) != null;
    } catch (Exception e) {
      return false;
    }
  }

  private JsonNode getJson(RedisModulesCommands<String, String> commands, String key) {
    try {
      Object res = commands.jsonGet(key, JsonPath.of("$"));
      if (res == null) {
        return NullNode.getInstance();
      }
      return MAPPER.valueToTree(res);
    } catch (Exception e) {
      return stringAsJson(commands, key);
    }
  }

  private JsonNode stringAsJson(RedisStringCommands<String, String> commands, String key) {
    String v = commands.get(key);
    return v == null ? NullNode.getInstance() : TextNode.valueOf(v);
  }

  private JsonNode hashAsJson(RedisHashCommands<String, String> commands, String key) {
    Map<String, String> map = commands.hgetall(key);
    ObjectNode obj = MAPPER.createObjectNode();
    map.forEach(obj::put);
    return obj;
  }

  private JsonNode setAsJson(RedisSetCommands<String, String> commands, String key) {
    Set<String> members = commands.smembers(key);
    ArrayNode arr = MAPPER.createArrayNode();
    members.forEach(arr::add);
    return arr;
  }

  private JsonNode zsetAsJson(RedisSortedSetCommands<String, String> commands, String key) {
    List<ScoredValue<String>> list = commands.zrangeWithScores(key, 0, -1);
    ArrayNode arr = MAPPER.createArrayNode();
    for (ScoredValue<String> sv : list) {
      ArrayNode pair = MAPPER.createArrayNode();
      pair.add(sv.getScore());
      pair.add(sv.getValue());
      arr.add(pair);
    }
    return arr;
  }
}
