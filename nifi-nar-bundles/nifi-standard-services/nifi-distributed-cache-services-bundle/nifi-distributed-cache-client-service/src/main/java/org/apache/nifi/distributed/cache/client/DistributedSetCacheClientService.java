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
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.distributed.cache.protocol.ProtocolVersion;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.remote.StandardVersionNegotiatorFactory;
import org.apache.nifi.remote.VersionNegotiatorFactory;
import org.apache.nifi.ssl.SSLContextService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Tags({"distributed", "cache", "state", "set", "cluster"})
@SeeAlso(classNames = {"org.apache.nifi.distributed.cache.server.DistributedSetCacheServer", "org.apache.nifi.ssl.StandardSSLContextService"})
@CapabilityDescription("Cung cấp khả năng giao tiếp với DistributedSetCacheServer. Điều này có thể được sử dụng để chia sẻ một Set "
        + "giữa các nút trong một cụm Life")
public class DistributedSetCacheClientService extends AbstractControllerService implements DistributedSetCacheClient {

    public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
            .name("Server Hostname")
            .description("Tên của máy chủ đang chạy dịch vụ DistributedSetCacheServer")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("Server Port")
            .description("Cổng trên máy chủ từ xa sẽ được sử dụng khi giao tiếp với dịch vụ DistributedSetCacheServer")
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
            .description("Chỉ định thời gian chờ khi giao tiếp với máy chủ từ xa trước khi xác định "
                    + "rằng đã xảy ra lỗi giao tiếp nếu không thể gửi hoặc nhận dữ liệu")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("30 secs")
            .build();

    /**
     * The implementation of the business logic for {@link DistributedSetCacheClientService}.
     */
    private volatile NettyDistributedSetCacheClient cacheClient = null;

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
        getLogger().debug("Enabling Set Cache Client Service [{}]", context.getName());
        this.versionNegotiatorFactory = new StandardVersionNegotiatorFactory(ProtocolVersion.V1.value());
        this.cacheClient = new NettyDistributedSetCacheClient(
                context.getProperty(HOSTNAME).getValue(),
                context.getProperty(PORT).asInteger(),
                context.getProperty(COMMUNICATIONS_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue(),
                context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class),
                versionNegotiatorFactory,
                this.getIdentifier());
    }

    @OnDisabled
    public void onDisabled() throws IOException {
        getLogger().debug("Disabling Set Cache Client Service");
        this.cacheClient.close();
        this.versionNegotiatorFactory = null;
        this.cacheClient = null;
    }

    @OnStopped
    public void onStopped() throws IOException {
        if (isEnabled()) {
            onDisabled();
        }
    }

    @Override
    public <T> boolean addIfAbsent(T value, Serializer<T> serializer) throws IOException {
        final byte[] bytes = CacheClientSerde.serialize(value, serializer);
        return cacheClient.addIfAbsent(bytes);
    }

    @Override
    public <T> boolean contains(T value, Serializer<T> serializer) throws IOException {
        final byte[] bytes = CacheClientSerde.serialize(value, serializer);
        return cacheClient.contains(bytes);
    }

    @Override
    public <T> boolean remove(T value, Serializer<T> serializer) throws IOException {
        final byte[] bytes = CacheClientSerde.serialize(value, serializer);
        return cacheClient.remove(bytes);
    }

    @Override
    public void close() throws IOException {
        if (isEnabled()) {
            onDisabled();
        }
    }
}
