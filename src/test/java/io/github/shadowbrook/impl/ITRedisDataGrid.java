package io.github.shadowbrook.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.shadowbrook.RedisDataGrid;
import io.github.shadowbrook.RedisInstance;
import io.github.shadowbrook.RedisTestContainerFactory;
import io.github.shadowbrook.config.RedisConfig;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class ITRedisDataGrid {
  @Container public GenericContainer<?> redis = RedisTestContainerFactory.newContainer();

  private RedisConfig config;

  @BeforeEach
  void beforeEach() {
    String redisUrl = "redis://" + redis.getHost() + ":" + redis.getFirstMappedPort();
    config = new RedisConfig().addEndpoint(redisUrl);
  }

  @Test
  void createDataGrid() {
    RedisDataGrid dataGrid = assertDoesNotThrow(() -> RedisDataGrid.create(Vertx.vertx(), config));
    assertThat(dataGrid).isInstanceOf(RedisInstance.class);
    assertTrue(((RedisInstance) dataGrid).ping());
  }
}
