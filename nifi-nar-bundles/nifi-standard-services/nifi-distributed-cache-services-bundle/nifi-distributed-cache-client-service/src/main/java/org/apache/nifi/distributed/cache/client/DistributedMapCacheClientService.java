/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.distributed.cache.client;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.distributed.cache.client.adapter.AtomicCacheEntryInboundAdapter;
import org.apache.nifi.distributed.cache.client.adapter.MapInboundAdapter;
import org.apache.nifi.distributed.cache.client.adapter.MapValuesInboundAdapter;
import org.apache.nifi.distributed.cache.client.adapter.SetInboundAdapter;
import org.apache.nifi.distributed.cache.client.adapter.ValueInboundAdapter;
import org.apache.nifi.distributed.cache.protocol.ProtocolVersion;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.remote.StandardVersionNegotiatorFactory;
import org.apache.nifi.remote.VersionNegotiatorFactory;
import org.apache.nifi.ssl.SSLContextService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Tags({"distributed", "cache", "state", "map", "cluster"})
@SeeAlso(classNames = {"org.apache.nifi.distributed.cache.server.map.DistributedMapCacheServer", "org.apache.nifi.ssl.StandardSSLContextService"})
@CapabilityDescription("Cung cấp khả năng giao tiếp với DistributedMapCacheServer. Điều này có thể được sử dụng để chia sẻ một Map "
    + "giữa các nút trong một cụm Life")
public class DistributedMapCacheClientService extends AbstractControllerService implements AtomicDistributedMapCacheClient<Long> {

    private static final long DEFAULT_CACHE_REVISION = 0L;

    public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
        .name("Server Hostname")
        .description("Tên của máy chủ đang chạy dịch vụ DistributedMapCacheServer")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
        .name("Server Port")
        .description("Cổng trên máy chủ từ xa sẽ được sử dụng khi giao tiếp với dịch vụ DistributedMapCacheServer")
        .required(true)
        .addValidator(StandardValidators.PORT_VALIDATOR)
        .defaultValue("4557")
        .build();
    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
        .name("SSL Context Service")
        .description("Nếu được chỉ định, cho biết SSL Context Service được sử dụng để giao tiếp với "
                + "máy chủ từ xa. Nếu không được chỉ định, các giao tiếp sẽ không được mã hóa")
        .required(false)
        .identifiesControllerService(SSLContextService.class)
        .build();
    public static final PropertyDescriptor COMMUNICATIONS_TIMEOUT = new PropertyDescriptor.Builder()
        .name("Communications Timeout")
        .description("Chỉ định thời gian chờ khi giao tiếp với máy chủ từ xa trước khi xác định rằng "
                + "có lỗi giao tiếp nếu không thể gửi hoặc nhận dữ liệu")
        .required(true)
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .defaultValue("30 secs")
        .build();
    /**
     * The implementation of the business logic for {@link DistributedMapCacheClientService}.
     */
    private volatile NettyDistributedMapCacheClient cacheClient = null;

    /**
     * Creator of object used to broker the version of the distributed cache protocol with the service.
     */
    private volatile VersionNegotiatorFactory versionNegotiatorFactory = null;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(HOSTNAME);
        descriptors.add(PORT);
        descriptors.add(SSL_CONTEXT_SERVICE);
        descriptors.add(COMMUNICATIONS_TIMEOUT);
        return descriptors;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        getLogger().debug("Enabling Map Cache Client Service [{}]", context.getName());
        this.versionNegotiatorFactory  = new StandardVersionNegotiatorFactory(
                ProtocolVersion.V3.value(), ProtocolVersion.V2.value(), ProtocolVersion.V1.value());
        this.cacheClient = new NettyDistributedMapCacheClient(
                context.getProperty(HOSTNAME).getValue(),
                context.getProperty(PORT).asInteger(),
                context.getProperty(COMMUNICATIONS_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue(),
                context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class),
                versionNegotiatorFactory,
                this.getIdentifier(),
                getLogger());
    }

    @OnShutdown
    @OnDisabled
    public void onDisabled() throws IOException {
        if (cacheClient != null) {
            this.cacheClient.close();
        }
        this.versionNegotiatorFactory = null;
        this.cacheClient = null;
    }

    @Override
    public <K, V> boolean putIfAbsent(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
        final byte[] bytesKey = CacheClientSerde.serialize(key, keySerializer);
        final byte[] bytesValue = CacheClientSerde.serialize(value, valueSerializer);
        return cacheClient.putIfAbsent(bytesKey, bytesValue);
    }

    @Override
    public <K, V> void put(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
        final byte[] bytesKey = CacheClientSerde.serialize(key, keySerializer);
        final byte[] bytesValue = CacheClientSerde.serialize(value, valueSerializer);
        cacheClient.put(bytesKey, bytesValue);
    }

    @Override
    public <K> boolean containsKey(final K key, final Serializer<K> keySerializer) throws IOException {
        final byte[] bytesKey = CacheClientSerde.serialize(key, keySerializer);
        return cacheClient.containsKey(bytesKey);
    }

    @Override
    public <K, V> V getAndPutIfAbsent(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer, final Deserializer<V> valueDeserializer) throws IOException {
        final byte[] bytesKey = CacheClientSerde.serialize(key, keySerializer);
        final byte[] bytesValue = CacheClientSerde.serialize(value, valueSerializer);
        final ValueInboundAdapter<V> inboundAdapter = new ValueInboundAdapter<>(valueDeserializer);
        return cacheClient.getAndPutIfAbsent(bytesKey, bytesValue, inboundAdapter);
    }

    @Override
    public <K, V> V get(final K key, final Serializer<K> keySerializer, final Deserializer<V> valueDeserializer) throws IOException {
        final byte[] bytesKey = CacheClientSerde.serialize(key, keySerializer);
        final ValueInboundAdapter<V> inboundAdapter = new ValueInboundAdapter<>(valueDeserializer);
        return cacheClient.get(bytesKey, inboundAdapter);
    }

    @Override
    public <K, V> Map<K, V> subMap(Set<K> keys, Serializer<K> keySerializer, Deserializer<V> valueDeserializer) throws IOException {
        Collection<byte[]> bytesKeys = CacheClientSerde.serialize(keys, keySerializer);
        final MapValuesInboundAdapter<K, V> inboundAdapter =
                new MapValuesInboundAdapter<>(keys, valueDeserializer, new HashMap<>());
        return cacheClient.subMap(bytesKeys, inboundAdapter);
    }

    @Override
    public <K> boolean remove(final K key, final Serializer<K> serializer) throws IOException {
        final byte[] bytesKey = CacheClientSerde.serialize(key, serializer);
        return cacheClient.remove(bytesKey);
    }

    @Override
    public <K, V> V removeAndGet(K key, Serializer<K> keySerializer, Deserializer<V> valueDeserializer) throws IOException {
        final byte[] bytesKey = CacheClientSerde.serialize(key, keySerializer);
        final ValueInboundAdapter<V> inboundAdapter = new ValueInboundAdapter<>(valueDeserializer);
        return cacheClient.removeAndGet(bytesKey, inboundAdapter);
    }

    @Override
    public long removeByPattern(String regex) throws IOException {
        return cacheClient.removeByPattern(regex);
    }

    @Override
    public <K, V> Map<K, V> removeByPatternAndGet(String regex, Deserializer<K> keyDeserializer,
                                                  Deserializer<V> valueDeserializer) throws IOException {
        final MapInboundAdapter<K, V> inboundAdapter =
                new MapInboundAdapter<>(keyDeserializer, valueDeserializer, new HashMap<>());
        return cacheClient.removeByPatternAndGet(regex, inboundAdapter);
    }

    @Override
    public <K, V> AtomicCacheEntry<K, V, Long> fetch(final K key, final Serializer<K> keySerializer,
                                                     final Deserializer<V> valueDeserializer) throws IOException {
        final byte[] bytesKey = CacheClientSerde.serialize(key, keySerializer);
        final AtomicCacheEntryInboundAdapter<K, V> inboundAdapter =
                new AtomicCacheEntryInboundAdapter<>(key, valueDeserializer);
        return cacheClient.fetch(bytesKey, inboundAdapter);
    }

    @Override
    public <K, V> boolean replace(AtomicCacheEntry<K, V, Long> entry, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
        final byte[] bytesKey = CacheClientSerde.serialize(entry.getKey(), keySerializer);
        final byte[] bytesValue = CacheClientSerde.serialize(entry.getValue(), valueSerializer);
        final long revision = entry.getRevision().orElse(DEFAULT_CACHE_REVISION);
        return cacheClient.replace(bytesKey, bytesValue, revision);
    }

    @Override
    public <K> Set<K> keySet(Deserializer<K> keyDeserializer) throws IOException {
        final SetInboundAdapter<K> inboundAdapter = new SetInboundAdapter<>(keyDeserializer, new HashSet<>());
        return cacheClient.keySet(inboundAdapter);
    }

    @Override
    public void close() throws IOException {
        if (isEnabled()) {
            onDisabled();
        }
    }
}
