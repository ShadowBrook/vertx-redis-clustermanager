package io.github.shadowbrook;

import static com.jayway.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.shadowbrook.config.RedisConfig;
import io.github.shadowbrook.impl.codec.RedisMapCodec;
import io.vertx.core.Vertx;
import io.vertx.core.spi.cluster.RegistrationInfo;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RSetMultimap;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Integration tests for the subscription reconciliation wiring in {@link RedisClusterManager}.
 *
 * <p>The subscriptions key namespace is empty in these tests, making the Redis key
 * <code>__vertx:subs</code>.
 */
@Testcontainers
class ITRedisClusterManagerReconcile {

  private static final String SUBS_KEY = "__vertx:subs";
  private static final String ADDRESS = "reconcile-test-address";

  @Container public GenericContainer<?> redis = RedisTestContainerFactory.newContainer();

  private Vertx vertx;
  private RedissonClient redisson;

  @AfterEach
  void afterEach() throws Exception {
    if (vertx != null) {
      vertx.close().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
    }
    if (redisson != null) {
      redisson.shutdown();
    }
  }

  /**
   * Start a clustered Vert.x instance with the given reconciliation interval and a direct Redisson
   * client (same codec as the cluster manager) for manipulating subscriptions in Redis directly.
   */
  private RedisClusterManager startClusterManager(long reconcileIntervalMs) throws Exception {
    String redisUrl = "redis://" + redis.getHost() + ":" + redis.getFirstMappedPort();
    RedisConfig config =
        new RedisConfig()
            .addEndpoint(redisUrl)
            .setSubscriptionReconcileIntervalMs(reconcileIntervalMs);
    RedisClusterManager clusterManager = new RedisClusterManager(config);

    CountDownLatch started = new CountDownLatch(1);
    Vertx.builder()
        .withClusterManager(clusterManager)
        .buildClustered()
        .onSuccess(
            v -> {
              vertx = v;
              started.countDown();
            });
    assertTrue(started.await(30, TimeUnit.SECONDS), "Clustered Vert.x did not start");

    Config directConfig = new Config();
    directConfig.useSingleServer().setAddress(redisUrl);
    directConfig.setCodec(new RedisMapCodec());
    redisson = Redisson.create(directConfig);
    return clusterManager;
  }

  private void registerConsumer() throws InterruptedException {
    CountDownLatch registered = new CountDownLatch(1);
    vertx
        .eventBus()
        .consumer(ADDRESS, msg -> {})
        .completion()
        .onSuccess(v -> registered.countDown());
    assertTrue(registered.await(30, TimeUnit.SECONDS), "Consumer registration did not complete");
  }

  private RSetMultimap<String, RegistrationInfo> subsMap() {
    return redisson.getSetMultimap(SUBS_KEY);
  }

  /** Remove the registration directly in Redis, bypassing the catalog. */
  private void simulateSilentRedisLoss() {
    List<RegistrationInfo> lost = List.copyOf(subsMap().getAll(ADDRESS));
    assertFalse(lost.isEmpty(), "Expected the registration to be present in Redis");
    lost.forEach(info -> assertTrue(subsMap().remove(ADDRESS, info)));
  }

  @Test
  void isSubscriptionVisibleWhenInactive() {
    RedisClusterManager clusterManager = RedisClusterManagerTestFactory.newInstance(redis);
    assertFalse(clusterManager.isSubscriptionVisible(ADDRESS));
  }

  @Test
  void periodicReconcileRestoresLostSubscription() throws Exception {
    RedisClusterManager clusterManager = startClusterManager(300);
    registerConsumer();
    assertTrue(clusterManager.isSubscriptionVisible(ADDRESS));

    simulateSilentRedisLoss();
    assertFalse(clusterManager.isSubscriptionVisible(ADDRESS));

    // The periodic reconciliation timer must restore the lost subscription.
    await().atMost(15, TimeUnit.SECONDS).until(() -> clusterManager.isSubscriptionVisible(ADDRESS));
  }

  @Test
  void reconcileNowRestoresLostSubscription() throws Exception {
    // Interval 0 disables the periodic reconciliation timer.
    RedisClusterManager clusterManager = startClusterManager(0);
    registerConsumer();
    assertTrue(clusterManager.isSubscriptionVisible(ADDRESS));

    simulateSilentRedisLoss();
    assertFalse(clusterManager.isSubscriptionVisible(ADDRESS));

    clusterManager.reconcileNow();
    await().atMost(15, TimeUnit.SECONDS).until(() -> clusterManager.isSubscriptionVisible(ADDRESS));
  }
}
