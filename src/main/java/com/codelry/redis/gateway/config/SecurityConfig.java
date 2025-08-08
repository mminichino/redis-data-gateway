package com.codelry.redis.gateway.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;

import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;

@Configuration
public class SecurityConfig {

  @Bean
  public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
    // Anonymous + Basic + JWT bearer enabled; endpoints left open by default
    http
        .csrf(csrf -> csrf.disable())
        .authorizeHttpRequests(auth -> auth
            .requestMatchers("/actuator/health", "/actuator/info").permitAll()
            .anyRequest().permitAll()
        )
        .httpBasic(Customizer.withDefaults())
        .oauth2ResourceServer(oauth2 -> oauth2.jwt(Customizer.withDefaults()))
        .anonymous(Customizer.withDefaults());

    return http.build();
  }

  // Optional: Simple in-memory user for Basic auth (override via properties)
  @Bean
  public UserDetailsService userDetailsService(
      @Value("${app.security.basic.username:user}") String username,
      @Value("${app.security.basic.password:changeit}") String password
  ) {
    PasswordEncoder encoder = passwordEncoder();
    return new InMemoryUserDetailsManager(
        User.withUsername(username)
            .password(encoder.encode(password))
            .roles("USER")
            .build()
    );
  }

  @Bean
  public PasswordEncoder passwordEncoder() {
    return PasswordEncoderFactories.createDelegatingPasswordEncoder();
  }

  // JWT decoder:
  // - If app.security.jwt.jwk-set-uri is set, use JWK set (asymmetric keys)
  // - Else if app.security.jwt.secret is set, use HMAC secret
  @Bean
  public JwtDecoder jwtDecoder(
      @Value("${app.security.jwt.jwk-set-uri:}") String jwkSetUri,
      @Value("${app.security.jwt.secret:}") String hmacSecret
  ) {
    if (jwkSetUri != null && !jwkSetUri.isBlank()) {
      return NimbusJwtDecoder.withJwkSetUri(jwkSetUri).build();
    }
    if (hmacSecret != null && !hmacSecret.isBlank()) {
      byte[] keyBytes = hmacSecret.getBytes(StandardCharsets.UTF_8);
      SecretKeySpec key = new SecretKeySpec(keyBytes, "HmacSHA256");
      return NimbusJwtDecoder.withSecretKey(key).build();
    }
    // If neither configured, create a decoder that will reject JWTs by default.
    // You can also throw here to force configuration.
    byte[] keyBytes = "please-configure-a-secret".getBytes(StandardCharsets.UTF_8);
    SecretKeySpec key = new SecretKeySpec(keyBytes, "HmacSHA256");
    return NimbusJwtDecoder.withSecretKey(key).build();
  }
}
