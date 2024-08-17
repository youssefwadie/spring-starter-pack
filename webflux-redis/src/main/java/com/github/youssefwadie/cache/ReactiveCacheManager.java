package com.github.youssefwadie.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.util.Assert;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * A simple wrapper around {@link CacheManager}, providing reactive caching capabilities.
 */
public class ReactiveCacheManager {
    private static final Logger LOG = LoggerFactory.getLogger(ReactiveCacheManager.class);

    private final CacheManager cacheManager;

    /**
     * Creates a new {@link ReactiveCacheManager} instance with the given {@link CacheManager}.
     *
     * @param cacheManager The {@link CacheManager} to use.
     * @throws IllegalArgumentException If the {@link CacheManager} is null.
     */
    public ReactiveCacheManager(CacheManager cacheManager) {
        Assert.notNull(cacheManager, "CacheManager must not be null");
        this.cacheManager = cacheManager;
    }

    /**
     * Aggregates the elements of the given {@link Flux} into a {@link List}, and caches the list in the specified cache.
     *
     * @param cacheName The name of the cache to store the list in.
     * @param key       The key to store the list under.
     * @param source    The {@link Flux} to cache.
     * @param <T>       The type of the elements emitted by the source {@link Flux}, must be a {@link Serializable}.
     * @return A new {@link Flux} that emits the same elements as the source {@link Flux}.
     */
    public <T extends Serializable> Flux<T> cacheFlux(String cacheName, Object key, Flux<T> source) {
        Assert.hasText(cacheName, "Cache name must not be empty");
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(source, "Source Flux must not be null");

        return source.collectList()
                .doOnSuccess(elements -> {
                    Cache cache = cacheManager.getCache(cacheName);
                    if (cache != null) {
                        cache.put(key, elements);
                    }
                })
                .flatMapMany(Flux::fromIterable);
    }

    /**
     * Caches the value of the given {@link Mono} in the specified cache.
     *
     * @param cacheName The name of the cache to store the value in.
     * @param key       The key to store the value under.
     * @param source    The {@link Mono} to cache.
     * @param <T>       The type of the value emitted by the source {@link Mono}, must be a {@link Serializable}.
     * @return A new {@link Mono} that emits the same value as the source {@link Mono}.
     * @throws IllegalArgumentException If the cache name, key, or source {@link Mono} is null.
     */
    public <T extends Serializable> Mono<T> cacheMono(String cacheName, Object key, Mono<T> source) {
        Assert.hasText(cacheName, "Cache name must not be empty");
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(source, "Source Flux must not be null");
        return source.doOnSuccess(value -> {
            var cache = cacheManager.getCache(cacheName);
            if (cache != null) {
                cache.put(key, value);
            }
        });
    }

    /**
     * Returns a cached {@link Flux} from the specified cache, or computes and caches the value using the given {@link Supplier} if the cache does not contain the value.
     *
     * @param cacheName    The name of the cache to get the value from.
     * @param key          The key to get the value under.
     * @param fluxSupplier The {@link Supplier} to compute the value if the cache does not contain it.
     * @param <T>          The type of the value emitted by the {@link Flux}, must be a {@link Serializable}.
     * @return A {@link Flux} that emits the cached value, or the value computed by the supplier if the cache does not contain it.
     */
    @SuppressWarnings("unchecked")
    public <T extends Serializable> Flux<T> getCachedFluxOrElseGet(String cacheName, Object key, Supplier<Flux<T>> fluxSupplier) {

        Assert.hasText(cacheName, "Cache name must not be empty");
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(fluxSupplier, "Flux supplier must not be null");

        Cache cache = cacheManager.getCache(cacheName);
        if (cache == null) {
            LOG.warn("Cache {} not found", cacheName);
            return fluxSupplier.get();
        }

        Cache.ValueWrapper valueWrapper = cache.get(key);
        if (valueWrapper != null) {
            return Flux.fromIterable((List<T>) Objects.requireNonNull(valueWrapper.get()));
        }
        return cacheFlux(cacheName, key, fluxSupplier.get());
    }

    /**
     * Returns a cached {@link Mono} from the specified cache, or computes and caches the value using the given {@link Supplier} if the cache does not contain the value.
     *
     * @param cacheName    The name of the cache to get the value from.
     * @param key          The key to get the value under.
     * @param monoSupplier The {@link Supplier} to compute the value if the cache does not contain it.
     * @param <T>          The type of the value emitted by the {@link Mono}, must be a {@link Serializable}.
     * @return A {@link Mono} that emits the cached value, or the value computed by the supplier if the cache does not contain it.
     * @throws IllegalArgumentException If the cache name, key, or supplier is null.
     */
    @SuppressWarnings("unchecked")
    public <T extends Serializable> Mono<T> getCachedMonoOrElseGet(String cacheName, Object key, Supplier<Mono<T>> monoSupplier) {
        Assert.hasText(cacheName, "Cache name must not be empty");
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(monoSupplier, "Mono supplier must not be null");
        Cache cache = cacheManager.getCache(cacheName);
        if (cache == null) {
            LOG.warn("Cache {} not found", cacheName);
            return monoSupplier.get();
        }

        Cache.ValueWrapper valueWrapper = cache.get(key);
        if (valueWrapper != null) {
            return Mono.just((T) Objects.requireNonNull(valueWrapper.get()));
        }
        return cacheMono(cacheName, key, monoSupplier.get());
    }

    /**
     * Evicts the entry with the given key from the specified cache.
     *
     * @param cacheName The name of the cache to evict the entry from.
     * @param key       The key of the entry to evict.
     * @throws IllegalArgumentException If the cache name or key is null.
     */
    public void evict(String cacheName, Object key) {
        Assert.hasText(cacheName, "Cache name must not be empty");
        Assert.notNull(key, "Key must not be null");
        Cache cache = cacheManager.getCache(cacheName);

        if (cache != null) {
            cache.evict(key);
        }
    }

    /**
     * Evicts all entries from the specified cache.
     *
     * @param cacheName The name of the cache to evict all entries from.
     * @throws IllegalArgumentException If the cache name is null.
     */
    public void evictAll(String cacheName) {
        Assert.hasText(cacheName, "Cache name must not be empty");
        Cache cache = cacheManager.getCache(cacheName);

        if (cache != null) {
            cache.clear();
        }
    }

    /**
     * Checks if the specified cache contains an entry with the given key.
     *
     * @param cacheName The name of the cache to check.
     * @param key       The key to check.
     * @return {@code true} if the cache contains an entry with the given key, {@code false} otherwise.
     * @throws IllegalArgumentException If the cache name or key is null.
     */
    public boolean contains(String cacheName, Object key) {
        Assert.hasText(cacheName, "Cache name must not be empty");
        Assert.notNull(key, "Key must not be null");
        Cache cache = cacheManager.getCache(cacheName);
        if (cache != null) {
            return cache.get(key) != null;
        }
        return false;
    }

    /**
     * Returns the underlying {@link CacheManager} instance.
     *
     * @return The underlying {@link CacheManager} instance.
     */
    public CacheManager getCacheManager() {
        return cacheManager;
    }
}
