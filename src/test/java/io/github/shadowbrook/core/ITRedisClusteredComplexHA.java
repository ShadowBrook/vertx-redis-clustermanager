package io.github.shadowbrook.core;

import io.github.shadowbrook.RedisClusterManagerTestFactory;
import io.github.shadowbrook.RedisTestContainerFactory;
import io.vertx.tests.ha.ComplexHATest;
import io.vertx.core.spi.cluster.ClusterManager;
import org.junit.Rule;
import org.testcontainers.containers.GenericContainer;

public class ITRedisClusteredComplexHA extends ComplexHATest {
  @Rule public GenericContainer<?> redis = RedisTestContainerFactory.newContainer();

  @Override
  protected ClusterManager getClusterManager() {
    return RedisClusterManagerTestFactory.newInstance(redis);
  }
}
