package com.codelry.redis.gateway.service;

import com.codelry.redis.gateway.grpc.*;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.search.Document;
import com.redis.lettucemod.search.SearchResults;
import com.redis.lettucemod.search.SearchOptions;
import io.grpc.Status;
import io.lettuce.core.json.JsonPath;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;

import java.util.*;
import java.util.stream.Collectors;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@GrpcService
public class RedisGatewayService extends RedisGatewayGrpc.RedisGatewayImplBase {

  private static final Logger logger = LoggerFactory.getLogger(RedisGatewayService.class);

  private final RedisTemplate<String, String> redisTemplate;
  private final RedisModulesCommands<String, String> modulesCommands;

  @Autowired
  public RedisGatewayService(RedisTemplate<String, String> redisTemplate, StatefulRedisModulesConnection<String, String> modulesConnection) {
    this.redisTemplate = redisTemplate;
    this.modulesCommands = modulesConnection.sync();
  }

  @Override
  public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
    try {
      redisTemplate.opsForValue().set(request.getKey(), request.getValue());
      PutResponse response = PutResponse.newBuilder().setSuccess(true).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
    try {
      String value = redisTemplate.opsForValue().get(request.getKey());
      GetResponse response = GetResponse.newBuilder().setValue(value == null ? "" : value).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
    try {
      Long deletedCount = redisTemplate.delete(request.getKeysList());
      DeleteResponse response = DeleteResponse.newBuilder()
          .setSuccess(true)
          .setDeletedCount(deletedCount)
          .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void hSet(HSetRequest request, StreamObserver<PutResponse> responseObserver) {
    try {
      redisTemplate.opsForHash().put(request.getKey(), request.getField(), request.getValue());
      PutResponse response = PutResponse.newBuilder().setSuccess(true).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void hGet(HGetRequest request, StreamObserver<GetResponse> responseObserver) {
    try {
      String value = (String) redisTemplate.opsForHash().get(request.getKey(), request.getField());
      GetResponse response = GetResponse.newBuilder().setValue(value == null ? "" : value).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void hGetAll(HGetAllRequest request, StreamObserver<HGetAllResponse> responseObserver) {
    try {
      Map<Object, Object> fields = redisTemplate.opsForHash().entries(request.getKey());
      Map<String, String> stringFields = fields.entrySet().stream()
          .collect(Collectors.toMap(e -> (String) e.getKey(), e -> (String) e.getValue()));
      HGetAllResponse response = HGetAllResponse.newBuilder().putAllFields(stringFields).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void sAdd(SAddRequest request, StreamObserver<PutResponse> responseObserver) {
    try {
      redisTemplate.opsForSet().add(request.getKey(), request.getMembersList().toArray(new String[0]));
      PutResponse response = PutResponse.newBuilder().setSuccess(true).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void sMembers(SMembersRequest request, StreamObserver<SMembersResponse> responseObserver) {
    try {
      Set<String> members = redisTemplate.opsForSet().members(request.getKey());
      SMembersResponse response = SMembersResponse.newBuilder().addAllMembers(members == null ? Set.of() : members).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void zAdd(ZAddRequest request, StreamObserver<PutResponse> responseObserver) {
    try {
      Set<ZSetOperations.TypedTuple<String>> tuples = request.getMembersList().stream()
          .map(sm -> ZSetOperations.TypedTuple.of(sm.getMember(), sm.getScore()))
          .collect(Collectors.toSet());
      redisTemplate.opsForZSet().add(request.getKey(), tuples);
      PutResponse response = PutResponse.newBuilder().setSuccess(true).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void zRange(ZRangeRequest request, StreamObserver<ZRangeResponse> responseObserver) {
    try {
      Set<String> members = redisTemplate.opsForZSet().range(request.getKey(), request.getStart(), request.getStop());
      ZRangeResponse response = ZRangeResponse.newBuilder().addAllMembers(members == null ? Set.of() : members).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void vectorAdd(VectorAddRequest request, StreamObserver<PutResponse> responseObserver) {
    try {
      float[] vectorArray = new float[request.getVectorList().size()];
      for (int i = 0; i < request.getVectorList().size(); i++) {
        vectorArray[i] = request.getVectorList().get(i);
      }

      byte[] vectorBytes = floatArrayToByteArray(vectorArray);
      String vectorBase64 = Base64.getEncoder().encodeToString(vectorBytes);

      Map<String, String> fields = new HashMap<>();
      fields.put(request.getField(), vectorBase64);

      fields.putAll(request.getMetadataMap());

      redisTemplate.opsForHash().putAll(request.getKey(), fields);

      PutResponse response = PutResponse.newBuilder().setSuccess(true).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void vectorSearch(VectorSearchRequest request, StreamObserver<VectorSearchResponse> responseObserver) {
    try {
      float[] queryVector = new float[request.getQueryVectorList().size()];
      for (int i = 0; i < request.getQueryVectorList().size(); i++) {
        queryVector[i] = request.getQueryVectorList().get(i);
      }

      byte[] queryVectorBytes = floatArrayToByteArray(queryVector);
      String queryVectorBase64 = Base64.getEncoder().encodeToString(queryVectorBytes);

      String vectorFieldName = request.hasVectorField() ? request.getVectorField() : "embedding";
      String vectorQuery = String.format("*=>[KNN %d @%s $BLOB AS vector_score]", 
          request.getK(), vectorFieldName);

      if (request.hasFilter() && !request.getFilter().trim().isEmpty()) {
        vectorQuery = "(" + request.getFilter() + ") " + vectorQuery;
      }

      SearchOptions<String, String> options = SearchOptions.<String, String> builder()
          .param("BLOB", queryVectorBase64)
          .returnFields("*", "vector_score")
          .sortBy(SearchOptions.SortBy.asc("vector_score"))
          .limit(0, request.getK())
          .dialect(2)
          .build();

      SearchResults<String, String> results = modulesCommands.ftSearch(
          request.getIndexName(),
          vectorQuery,
          options
      );

      List<VectorSearchResult> searchResults = new ArrayList<>();
      for (Document<String, String> doc : results) {
        VectorSearchResult.Builder resultBuilder = VectorSearchResult.newBuilder()
            .setKey(doc.getId());

        if (doc.containsKey("vector_score")) {
          try {
            double score = Double.parseDouble(doc.get("vector_score"));
            resultBuilder.setScore((float) score);
          } catch (NumberFormatException e) {
            resultBuilder.setScore(doc.getScore().floatValue());
          }
        } else {
          resultBuilder.setScore(doc.getScore().floatValue());
        }

        doc.forEach((field, value) -> {
          if (!field.equals("vector_score") && !field.equals(vectorFieldName)) {
            resultBuilder.putMetadata(field, String.valueOf(value));
          }
        });

        searchResults.add(resultBuilder.build());
      }

      VectorSearchResponse response = VectorSearchResponse.newBuilder()
          .addAllResults(searchResults)
          .setTotalResults(results.getCount())
          .build();

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void jsonSet(JsonSetRequest request, StreamObserver<PutResponse> responseObserver) {
    try {

      String result = modulesCommands.jsonSet(request.getKey(), JsonPath.of(request.getPath()), modulesCommands.getJsonParser().createJsonValue(request.getJson()));
      PutResponse response = PutResponse.newBuilder()
          .setSuccess("OK".equals(result))
          .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      logger.error("Error in jsonSet for key: {}, path: {}, json: {}", request.getKey(), request.getPath(), request.getJson(), e);
      responseObserver.onError(Status.INTERNAL
          .withDescription("Failed to set JSON: " + e.getMessage())
          .withCause(e)
          .asRuntimeException());
    }
  }

  @Override
  public void jsonGet(JsonGetRequest request, StreamObserver<JsonGetResponse> responseObserver) {
    try {
      String value = modulesCommands.jsonGet(request.getKey(), JsonPath.of(request.getPath())).toString();
      JsonGetResponse response = JsonGetResponse.newBuilder().setJson(value == null ? "" : value).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      logger.error("Error in jsonGet for key: {}, path: {}", request.getKey(), request.getPath(), e);
      responseObserver.onError(Status.INTERNAL
          .withDescription("Failed to get JSON: " + e.getMessage())
          .withCause(e)
          .asRuntimeException());
    }
  }

  private byte[] floatArrayToByteArray(float[] floats) {
    ByteBuffer buffer = ByteBuffer.allocate(floats.length * 4).order(ByteOrder.LITTLE_ENDIAN);
    for (float f : floats) {
      buffer.putFloat(f);
    }
    return buffer.array();
  }
}
