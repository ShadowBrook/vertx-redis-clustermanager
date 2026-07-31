package io.github.shadowbrook.ext.web.sstore;

import io.github.shadowbrook.RedisClusterManagerTestFactory;
import io.github.shadowbrook.RedisTestContainerFactory;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.ext.web.it.sstore.ClusteredSessionHandlerTest;
import org.junit.Rule;
import org.testcontainers.containers.GenericContainer;

public class ITRedisClusteredSessionHandler extends ClusteredSessionHandlerTest {
  @Rule public GenericContainer<?> redis = RedisTestContainerFactory.newContainer();

  @Override
  protected ClusterManager getClusterManager() {
    return RedisClusterManagerTestFactory.newInstance(redis);
  }
}
