package com.codelry.redis.gateway.config;

import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import net.devh.boot.grpc.server.interceptor.GlobalServerInterceptorRegistry;
import net.devh.boot.grpc.server.service.GrpcServiceDefinition;
import net.devh.boot.grpc.server.service.GrpcServiceDiscoverer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
public class GrpcConfig implements SmartLifecycle {

  private final boolean enabled;
  private final int port;
  private final GrpcServiceDiscoverer serviceDiscoverer;
  private final GlobalServerInterceptorRegistry interceptorRegistry;

  private volatile boolean running = false;
  private Server server;

  public GrpcConfig(
      @Value("${app.grpc.notls.enabled:false}") boolean enabled,
      @Value("${app.grpc.notls.port:9080}") int port,
      GrpcServiceDiscoverer serviceDiscoverer,
      GlobalServerInterceptorRegistry interceptorRegistry
  ) {
    this.enabled = enabled;
    this.port = port;
    this.serviceDiscoverer = serviceDiscoverer;
    this.interceptorRegistry = interceptorRegistry;
  }

  @Override
  public void start() {
    if (!enabled || running) {
      return;
    }
    try {
      NettyServerBuilder builder = NettyServerBuilder.forPort(port);

      List<GrpcServiceDefinition> services = (List<GrpcServiceDefinition>) serviceDiscoverer.findGrpcServices();
      var interceptors = interceptorRegistry.getServerInterceptors();

      for (GrpcServiceDefinition def : services) {
        ServerServiceDefinition ssd = def.getDefinition();
        if (!interceptors.isEmpty()) {
          ssd = ServerInterceptors.intercept(ssd, interceptors);
        }
        builder.addService(ssd);
      }

      server = builder.build().start();
      running = true;
    } catch (IOException e) {
      throw new IllegalStateException("Failed to start additional plaintext gRPC server on port " + port, e);
    }
  }

  @Override
  public void stop() {
    if (server != null) {
      server.shutdown();
      try {
        server.awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException ignored) {
        Thread.currentThread().interrupt();
      } finally {
        server = null;
        running = false;
      }
    }
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  @Override
  public int getPhase() {
    return 0;
  }

  @Override
  public boolean isAutoStartup() {
    return true;
  }

  @Override
  public void stop(Runnable callback) {
    stop();
    callback.run();
  }
}
