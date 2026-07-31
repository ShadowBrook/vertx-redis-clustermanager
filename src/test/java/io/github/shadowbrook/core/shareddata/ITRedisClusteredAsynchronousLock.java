package io.github.shadowbrook.core.shareddata;

import io.github.shadowbrook.RedisClusterManagerTestFactory;
import io.github.shadowbrook.RedisTestContainerFactory;
import io.vertx.tests.shareddata.ClusteredAsynchronousLockTest;
import io.vertx.core.spi.cluster.ClusterManager;
import org.junit.Rule;
import org.testcontainers.containers.GenericContainer;

public class ITRedisClusteredAsynchronousLock extends ClusteredAsynchronousLockTest {
  @Rule public GenericContainer<?> redis = RedisTestContainerFactory.newContainer();

  @Override
  protected ClusterManager getClusterManager() {
    return RedisClusterManagerTestFactory.newInstance(redis);
  }
}
