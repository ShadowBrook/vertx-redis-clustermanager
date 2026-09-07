package io.github.shadowbrook.impl;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.shadowbrook.RedisTestContainerFactory;
import io.vertx.core.spi.cluster.RegistrationInfo;
import io.vertx.core.spi.cluster.RegistrationListener;
import io.vertx.core.spi.cluster.RegistrationUpdateEvent;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RSet;
import org.redisson.api.RSetMultimap;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class ITSubscriptionCatalog {

  @Container public GenericContainer<?> redis = RedisTestContainerFactory.newContainer();

  private final RedisKeyFactory keyFactory = new RedisKeyFactory("test");
  private RegistrationListener registrationListener;
  private SubscriptionCatalog subsCatalog;
  private RedissonClient redisson;

  @BeforeEach
  void beforeEach() {
    registrationListener = mock(RegistrationListener.class);

    String redisUrl = "redis://" + redis.getHost() + ":" + redis.getFirstMappedPort();
    Config config = new Config();
    config.useSingleServer().setAddress(redisUrl);
    redisson = Redisson.create(config);
    subsCatalog = new SubscriptionCatalog(redisson, keyFactory, registrationListener);
    when(registrationListener.wantsUpdatesFor(anyString())).thenReturn(true);
  }

  private void putSubs() {
    subsCatalog.put("sub-1", new RegistrationInfo("node1", 1, false));
    subsCatalog.put("sub-2", new RegistrationInfo("node1", 2, false));
    subsCatalog.put("sub-1", new RegistrationInfo("node2", 3, false));
  }

  @Test
  void put() {
    putSubs();
    assertThat(subsCatalog.get("sub-1")).hasSize(2);
    verify(registrationListener, timeout(100).atLeast(1))
        .registrationsUpdated(any(RegistrationUpdateEvent.class));
  }

  @Test
  void remove() {
    putSubs();
    subsCatalog.remove("sub-1", new RegistrationInfo("node1", 1, false));
    verify(registrationListener, timeout(100).atLeast(2))
        .registrationsUpdated(any(RegistrationUpdateEvent.class));
  }

  @Test
  void removeLocal() {
    RegistrationInfo reg = new RegistrationInfo("node1", 1, true);
    subsCatalog.put("local-1", reg);
    assertThat(subsCatalog.get("local-1")).containsOnly(reg);

    subsCatalog.remove("local-1", reg);
    assertThat(subsCatalog.get("local-1")).isEmpty();
  }

  @Test
  void republishOwn() {
    subsCatalog.put("local-1", new RegistrationInfo("node1", 1, true));
    subsCatalog.put("remote-1", new RegistrationInfo("node1", 1, false));
    subsCatalog.republishOwnSubs();
    verify(registrationListener, timeout(100).atLeast(2))
        .registrationsUpdated(any(RegistrationUpdateEvent.class));
  }

  @Test
  void removeUnknownSubs() {
    putSubs();
    subsCatalog.put("sub-1", new RegistrationInfo("node3", 4, false));
    subsCatalog.put("sub-1", new RegistrationInfo("node4", 5, false));
    subsCatalog.removeUnknownSubs("node4", asList("node2", "node3"));
    assertThat(subsCatalog.get("sub-1"))
        .containsOnly(
            new RegistrationInfo("node2", 3, false),
            new RegistrationInfo("node3", 4, false),
            new RegistrationInfo("node4", 5, false));
    assertThat(subsCatalog.get("sub-2")).isEmpty();
    verify(registrationListener, timeout(100).atLeast(1))
        .registrationsUpdated(any(RegistrationUpdateEvent.class));
  }

  @Test
  void removeUnknownSubsNoOp() {
    putSubs();
    subsCatalog.removeUnknownSubs("node3", asList("node1", "node2"));
    assertThat(subsCatalog.get("sub-1")).hasSize(2);
    assertThat(subsCatalog.get("sub-2")).hasSize(1);
  }

  @Test
  void removeUnknownSubsBulkException() {
    putSubs();
    subsCatalog.put("sub-1", new RegistrationInfo("node3", 4, false));
    subsCatalog.put("sub-1", new RegistrationInfo("node4", 5, false));

    List<RegistrationInfo> subs1 = List.of(new RegistrationInfo("node1", 1, false));
    List<RegistrationInfo> subs2 = List.of(new RegistrationInfo("node1", 2, false));

    // Spy and mock the subs map to throw exception
    RSetMultimap<String, RegistrationInfo> subsMap =
        spy(redisson.getSetMultimap(keyFactory.vertx("subs")));
    @SuppressWarnings("unchecked")
    RSet<RegistrationInfo> setMock = mock(RSet.class);
    when(subsMap.get("sub-1")).thenReturn(setMock);
    when(setMock.removeAll(subs1)).thenThrow(new RuntimeException("TEST"));

    SubscriptionCatalog.bulkRemoveUnknownSubsByKey(Map.of("sub-1", subs1, "sub-2", subs2), subsMap);

    assertThat(subsCatalog.get("sub-1"))
        .containsOnly(
            new RegistrationInfo("node2", 3, false),
            new RegistrationInfo("node3", 4, false),
            new RegistrationInfo("node4", 5, false));
    assertThat(subsCatalog.get("sub-2")).isEmpty();
    verify(registrationListener, timeout(100).atLeast(1))
        .registrationsUpdated(any(RegistrationUpdateEvent.class));
  }

  @Test
  void reconcileOwnSubs() {
    RegistrationInfo reg1 = new RegistrationInfo("node1", 1, false);
    RegistrationInfo reg2 = new RegistrationInfo("node1", 2, false);
    subsCatalog.put("sub-1", reg1);
    subsCatalog.put("sub-2", reg2);

    // Simulate a silent loss on the Redis side (bypassing the catalog), e.g. a write that
    // failed during a Redis outage.
    RSetMultimap<String, RegistrationInfo> subsMap =
        redisson.getSetMultimap(keyFactory.vertx("subs"));
    assertThat(subsMap.remove("sub-1", reg1)).isTrue();
    assertThat(redisson.getSetMultimap(keyFactory.vertx("subs")).getAll("sub-1")).isEmpty();

    assertThat(subsCatalog.reconcileOwnSubs()).isEqualTo(1);
    assertThat(redisson.getSetMultimap(keyFactory.vertx("subs")).getAll("sub-1"))
        .containsExactly(reg1);

    // Nothing left to repair: no address must be reported as fixed.
    assertThat(subsCatalog.reconcileOwnSubs()).isEqualTo(0);
  }

  /**
   * Wrap the given Redisson client so that the returned subs map and topic objects throw an
   * exception for every method named in {@code failingMethods}. The set is live: clearing it
   * "repairs" the proxy and restores pass-through behaviour.
   */
  private RedissonClient failingWritesRedisson(
      RedissonClient delegate, Set<String> failingMethods) {
    return (RedissonClient)
        Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[] {RedissonClient.class},
            (proxy, method, args) -> {
              if (method.getName().equals("getSetMultimap")) {
                return failingProxy(
                    method.invoke(delegate, args), new Class<?>[] {RSetMultimap.class}, failingMethods);
              }
              if (method.getName().equals("getTopic")) {
                return failingProxy(
                    method.invoke(delegate, args), new Class<?>[] {RTopic.class}, failingMethods);
              }
              return method.invoke(delegate, args);
            });
  }

  private Object failingProxy(Object target, Class<?>[] interfaces, Set<String> failingMethods) {
    return Proxy.newProxyInstance(
        getClass().getClassLoader(),
        interfaces,
        (proxy, method, args) -> {
          if (failingMethods.contains(method.getName())) {
            throw new RuntimeException("Simulated Redis failure");
          }
          try {
            return method.invoke(target, args);
          } catch (InvocationTargetException e) {
            throw e.getCause();
          }
        });
  }

  @Test
  void putSurvivesRedisWriteFailure() {
    Set<String> failingMethods = new HashSet<>(Set.of("put", "publish"));
    SubscriptionCatalog catalog =
        new SubscriptionCatalog(
            failingWritesRedisson(redisson, failingMethods), keyFactory, registrationListener);
    RegistrationInfo reg = new RegistrationInfo("node1", 1, false);

    // The Redis write fails but must not propagate.
    assertDoesNotThrow(() -> catalog.put("sub-1", reg));
    assertThat(redisson.getSetMultimap(keyFactory.vertx("subs")).getAll("sub-1")).isEmpty();

    // The registration was kept in memory, so repairing the proxy lets reconciliation
    // write it to Redis.
    failingMethods.clear();
    assertThat(catalog.reconcileOwnSubs()).isEqualTo(1);
    assertThat(redisson.getSetMultimap(keyFactory.vertx("subs")).getAll("sub-1"))
        .containsExactly(reg);
    catalog.close();
  }

  @Test
  void removeSurvivesRedisWriteFailure() {
    Set<String> failingMethods = new HashSet<>(Set.of("remove", "publish"));
    SubscriptionCatalog catalog =
        new SubscriptionCatalog(
            failingWritesRedisson(redisson, failingMethods), keyFactory, registrationListener);
    RegistrationInfo reg = new RegistrationInfo("node1", 1, false);
    catalog.put("sub-1", reg);
    assertThat(redisson.getSetMultimap(keyFactory.vertx("subs")).getAll("sub-1"))
        .containsExactly(reg);

    // The Redis removal fails but must not propagate.
    assertDoesNotThrow(() -> catalog.remove("sub-1", reg));
    assertThat(redisson.getSetMultimap(keyFactory.vertx("subs")).getAll("sub-1"))
        .containsExactly(reg);
    catalog.close();
  }

  @Test
  void isRegisteredInRedis() {
    subsCatalog.put("sub-1", new RegistrationInfo("node1", 1, false));
    subsCatalog.put("sub-1", new RegistrationInfo("node2", 2, false));

    assertThat(subsCatalog.isRegisteredInRedis("sub-1", "node1")).isTrue();
    assertThat(subsCatalog.isRegisteredInRedis("sub-1", "node2")).isTrue();
    assertThat(subsCatalog.isRegisteredInRedis("sub-1", "node3")).isFalse();
    assertThat(subsCatalog.isRegisteredInRedis("sub-2", "node1")).isFalse();

    // localOnly registrations never reach Redis.
    subsCatalog.put("local-1", new RegistrationInfo("node1", 3, true));
    assertThat(subsCatalog.isRegisteredInRedis("local-1", "node1")).isFalse();
  }

  @Test
  void removeForAllNodes() {
    putSubs();
    subsCatalog.removeAllForNodes(singleton("node1"));
    assertThat(subsCatalog.get("sub-1")).doesNotContain(new RegistrationInfo("node1", 1, false));
    assertThat(subsCatalog.get("sub-2")).isEmpty();
  }

  @Test
  void close() {
    assertDoesNotThrow(subsCatalog::close);
  }
}
