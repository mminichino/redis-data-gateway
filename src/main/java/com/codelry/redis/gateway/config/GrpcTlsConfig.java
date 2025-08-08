package com.codelry.redis.gateway.config;

import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import net.devh.boot.grpc.server.serverfactory.GrpcServerConfigurer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.ByteArrayInputStream;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;

@Configuration
public class GrpcTlsConfig {

  private final RedisKeystoreService keystoreService;
  private final char[] keystorePassword;

  public GrpcTlsConfig(
      RedisKeystoreService keystoreService,
      @Value("${app.ssl.keystore.password:password}") String keystorePassword
  ) {
    this.keystoreService = keystoreService;
    this.keystorePassword = keystorePassword.toCharArray();
  }

  @Bean
  public GrpcServerConfigurer grpcServerTlsConfigurer() {
    return serverBuilder -> {
      if (serverBuilder instanceof NettyServerBuilder netty) {
        try {
          SslContext sslContext = buildServerSslContextFromRedis();
          netty.sslContext(sslContext);
        } catch (Exception e) {
          throw new IllegalStateException("Failed to configure gRPC TLS from Redis keystore", e);
        }
      }
    };
  }

  private SslContext buildServerSslContextFromRedis() throws Exception {
    byte[] ksBytes = keystoreService.ensureAndGetKeystoreBytes();

    KeyStore ks = KeyStore.getInstance(RedisKeystoreService.KEYSTORE_TYPE);
    ks.load(new ByteArrayInputStream(ksBytes), keystorePassword);

    String alias = ks.containsAlias(RedisKeystoreService.KEY_ALIAS)
        ? RedisKeystoreService.KEY_ALIAS
        : findFirstPrivateKeyAlias(ks);

    PrivateKey privateKey = (PrivateKey) ks.getKey(alias, keystorePassword);
    X509Certificate[] chain = toX509Chain(ks.getCertificateChain(alias));

    return GrpcSslContexts
        .configure(SslContextBuilder.forServer(privateKey, chain))
        .build();
  }

  private static String findFirstPrivateKeyAlias(KeyStore ks) throws Exception {
    var aliases = ks.aliases();
    while (aliases.hasMoreElements()) {
      String a = aliases.nextElement();
      if (ks.isKeyEntry(a)) {
        return a;
      }
    }
    throw new IllegalStateException("No private key entry found in keystore");
  }

  private static X509Certificate[] toX509Chain(java.security.cert.Certificate[] chain) {
    if (chain == null || chain.length == 0) return new X509Certificate[0];
    X509Certificate[] x = new X509Certificate[chain.length];
    for (int i = 0; i < chain.length; i++) {
      x[i] = (X509Certificate) chain[i];
    }
    return x;
  }
}
