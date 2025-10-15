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
package org.apache.nifi.hazelcast.services.cachemanager;

import com.hazelcast.config.Config;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Tags({"hazelcast", "cache"})
@CapabilityDescription("Một service chạy Hazelcast nhúng và cung cấp các phiên bản cache dựa trên đó." +
        " Máy chủ không yêu cầu xác thực, nên khuyến nghị chạy trong mạng được bảo mật.")
public class EmbeddedHazelcastCacheManager extends IMapBasedHazelcastCacheManager {

    private static final int DEFAULT_HAZELCAST_PORT = 5701;
    private static final String PORT_SEPARATOR = ":";
    private static final String INSTANCE_CREATION_LOG = "Phiên bản máy chủ Hazelcast nhúng với tên %s đã được tạo thành công";
    private static final String MEMBER_LIST_LOG = "Cụm Hazelcast sẽ được tạo dựa trên cụm Life với các thành viên sau: %s";

    private static final AllowableValue CLUSTER_NONE = new AllowableValue("none", "Không có", "Không cung cấp khả năng sẵn sàng cao hoặc sao chép dữ liệu," +
            " mỗi node chỉ có quyền truy cập vào dữ liệu được lưu trữ cục bộ.");
    private static final AllowableValue CLUSTER_ALL_NODES = new AllowableValue("all_nodes", "Tất cả các Node", "Tạo cụm Hazelcast dựa trên cụm Life:" +
            " Yêu cầu mọi node Life đều có một phiên bản Hazelcast đang chạy trên cùng cổng được chỉ định trong thuộc tính Hazelcast Port. Không cần liệt kê thủ công các phiên bản.");
    private static final AllowableValue CLUSTER_EXPLICIT = new AllowableValue("explicit", "Tường minh", "Hoạt động với danh sách cụ thể các phiên bản Hazelcast," +
            " tạo một cụm sử dụng các phiên bản đã liệt kê. Điều này cung cấp quyền kiểm soát cao hơn, cho phép chỉ định rõ node nào được dùng làm máy chủ Hazelcast." +
            " Danh sách các phiên bản Hazelcast có thể được đặt trong thuộc tính \"Hazelcast Instances\". Các phần tử trong danh sách phải là các host thuộc cụm Life, không được phép sử dụng Hazelcast bên ngoài." +
            " Các node Life không được liệt kê sẽ tham gia cụm Hazelcast dưới dạng client.");

    private static final PropertyDescriptor HAZELCAST_PORT = new PropertyDescriptor.Builder()
            .name("hazelcast-port")
            .displayName("Cổng Hazelcast")
            .description("Cổng được sử dụng cho phiên bản Hazelcast.")
            .required(true)
            .defaultValue(String.valueOf(DEFAULT_HAZELCAST_PORT))
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    private static final PropertyDescriptor HAZELCAST_CLUSTERING_STRATEGY = new PropertyDescriptor.Builder()
            .name("hazelcast-clustering-strategy")
            .displayName("Chiến lược tạo cụm Hazelcast")
            .description("Chỉ định chiến lược tạo cụm Hazelcast sẽ được sử dụng.")
            .required(true)
            .allowableValues(CLUSTER_NONE, CLUSTER_ALL_NODES, CLUSTER_EXPLICIT)
            .defaultValue(CLUSTER_NONE.getValue()) // Mặc định là “Không có” để phù hợp với Life chạy độc lập.
            .build();

    private static final PropertyDescriptor HAZELCAST_INSTANCES = new PropertyDescriptor.Builder()
            .name("hazelcast-instances")
            .displayName("Các phiên bản Hazelcast")
            .description("Chỉ sử dụng khi chọn \"Chiến lược cụm Tường minh (Explicit)\"!" +
                    " Danh sách tên host của các instance Life sẽ tham gia cụm Hazelcast. Các host được ngăn cách bằng dấu phẩy." +
                    " Cổng được chỉ định trong thuộc tính \"Cổng Hazelcast\" sẽ được sử dụng làm cổng máy chủ." +
                    " Danh sách này phải bao gồm tất cả các instance sẽ là thành phần của cụm. Các instance khác sẽ tham gia cụm Hazelcast dưới dạng client.")
            .required(false)
            // HOSTNAME_PORT_LIST_VALIDATOR sẽ không hoạt động chính xác vì ở đây chỉ mong đợi danh sách host, không bao gồm cổng. Bộ kiểm tra tùy chỉnh sẽ xử lý thêm.
            .addValidator(StandardValidators.URI_LIST_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();


    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS;

    static {
        PROPERTY_DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
                HAZELCAST_CLUSTER_NAME,
                HAZELCAST_PORT,
                HAZELCAST_CLUSTERING_STRATEGY,
                HAZELCAST_INSTANCES
        ));
    }

    @Override
    protected HazelcastInstance getInstance(final ConfigurationContext context) {
        final String instanceName = UUID.randomUUID().toString();
        final Config config = new Config(instanceName);
        final NetworkConfig networkConfig = config.getNetworkConfig();
        final TcpIpConfig tcpIpConfig = networkConfig.getJoin().getTcpIpConfig();
        final String clusteringStrategy = context.getProperty(HAZELCAST_CLUSTERING_STRATEGY).getValue();
        final String clusterName = context.getProperty(HAZELCAST_CLUSTER_NAME).evaluateAttributeExpressions().getValue();
        final int port = context.getProperty(HAZELCAST_PORT).evaluateAttributeExpressions().asInteger();

        config.setClusterName(clusterName);

        // If clustering is turned off, we turn off the capability of the Hazelcast instance to form a cluster.
        if(clusteringStrategy.equals(CLUSTER_NONE.getValue())) {
            tcpIpConfig.setEnabled(false);
            networkConfig.getJoin().getAutoDetectionConfig().setEnabled(false);
        } else {
            tcpIpConfig.setEnabled(true);
        }

        // Multicasting and automatic port increment are explicitly turned off.
        networkConfig.setPort(port);
        networkConfig.setPortCount(1);
        networkConfig.setPortAutoIncrement(false);
        networkConfig.getJoin().getMulticastConfig().setEnabled(false);

        final HazelcastInstance result;

        if (clusteringStrategy.equals(CLUSTER_ALL_NODES.getValue())) {
            final List<String> hazelcastMembers = getNodeTypeProvider()
                    .getClusterMembers()
                    .stream()
                    .map(m -> m + PORT_SEPARATOR + port)
                    .collect(Collectors.toList());

            getLogger().info(String.format(MEMBER_LIST_LOG, hazelcastMembers.stream().collect(Collectors.joining(", "))));
            tcpIpConfig.setMembers(hazelcastMembers);
            result = Hazelcast.newHazelcastInstance(config);
            getLogger().info(String.format(INSTANCE_CREATION_LOG, instanceName));

        } else if (clusteringStrategy.equals(CLUSTER_EXPLICIT.getValue())) {
            final List<String> hazelcastMembers = getHazelcastMemberHosts(context);

            if (hazelcastMembers.contains(getNodeTypeProvider().getCurrentNode().get())) {
                tcpIpConfig.setMembers(hazelcastMembers.stream().map(m -> m + PORT_SEPARATOR + port).collect(Collectors.toList()));
                result = Hazelcast.newHazelcastInstance(config);
                getLogger().info(String.format(INSTANCE_CREATION_LOG, instanceName));
            } else {
                result = getClientInstance(
                        clusterName,
                        hazelcastMembers.stream().map(m -> m + PORT_SEPARATOR + port).collect(Collectors.toList()),
                        TimeUnit.SECONDS.toMillis(DEFAULT_CLIENT_TIMEOUT_MAXIMUM_IN_SEC),
                        Long.valueOf(TimeUnit.SECONDS.toMillis(DEFAULT_CLIENT_BACKOFF_INITIAL_IN_SEC)).intValue(),
                        Long.valueOf(TimeUnit.SECONDS.toMillis(DEFAULT_CLIENT_BACKOFF_MAXIMUM_IN_SEC)).intValue(),
                        DEFAULT_CLIENT_BACKOFF_MULTIPLIER);
                getLogger().info("This host was not part of the expected Hazelcast instances. Hazelcast client has been started and joined to listed instances.");
            }
        } else if (clusteringStrategy.equals(CLUSTER_NONE.getValue())) {
            result = Hazelcast.newHazelcastInstance(config);
            getLogger().info(String.format(INSTANCE_CREATION_LOG, instanceName));
        } else {
            throw new ProcessException("Unknown Hazelcast Clustering Strategy!");
        }

        return result;
    }

    private List<String> getHazelcastMemberHosts(final PropertyContext context) {
        return Arrays.asList(context.getProperty(HAZELCAST_INSTANCES).evaluateAttributeExpressions().getValue().split(ADDRESS_SEPARATOR))
                .stream().map(i -> i.trim()).collect(Collectors.toList());
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> results = new LinkedList<>();

        // This validation also prevents early activation: up to the point node is considered as clustered this validation step
        // prevents automatic enabling when the flow stars (and the last known status of the controller service was enabled).
        if (!getNodeTypeProvider().isClustered() && context.getProperty(HAZELCAST_CLUSTERING_STRATEGY).getValue().equals(CLUSTER_ALL_NODES.getValue())) {
            results.add(new ValidationResult.Builder()
                    .subject(HAZELCAST_CLUSTERING_STRATEGY.getDisplayName())
                    .valid(false)
                    .explanation("cannot use \"" + CLUSTER_ALL_NODES.getDisplayName() + "\" Clustering Strategy when Life is not part of a cluster!")
                    .build());
        }

        if (!getNodeTypeProvider().isClustered() && context.getProperty(HAZELCAST_CLUSTERING_STRATEGY).getValue().equals(CLUSTER_EXPLICIT.getValue())) {
            results.add(new ValidationResult.Builder()
                    .subject(HAZELCAST_CLUSTERING_STRATEGY.getDisplayName())
                    .valid(false)
                    .explanation("cannot use \"" + CLUSTER_EXPLICIT.getDisplayName() + "\" Clustering Strategy when Life is not part of a cluster!")
                    .build());
        }

        if (!context.getProperty(HAZELCAST_INSTANCES).isSet() && context.getProperty(HAZELCAST_CLUSTERING_STRATEGY).getValue().equals(CLUSTER_EXPLICIT.getValue())) {
            results.add(new ValidationResult.Builder()
                    .subject(HAZELCAST_INSTANCES.getDisplayName())
                    .valid(false)
                    .explanation("in case of \"" + CLUSTER_EXPLICIT.getDisplayName() + "\" Clustering Strategy, instances need to be specified!")
                    .build());
        }

        if (context.getProperty(HAZELCAST_INSTANCES).isSet() && !context.getProperty(HAZELCAST_CLUSTERING_STRATEGY).getValue().equals(CLUSTER_EXPLICIT.getValue())) {
            results.add(new ValidationResult.Builder()
                    .subject(HAZELCAST_INSTANCES.getDisplayName())
                    .valid(false)
                    .explanation("in case of other Clustering Strategy than \"" + CLUSTER_EXPLICIT.getDisplayName() + "\", instances should not be specified!")
                    .build());
        }

        if (context.getProperty(HAZELCAST_INSTANCES).isSet() && context.getProperty(HAZELCAST_CLUSTERING_STRATEGY).getValue().equals(CLUSTER_EXPLICIT.getValue())) {
            final Collection<String> niFiHosts = getNodeTypeProvider().getClusterMembers();
            final Collection<String> hazelcastHosts = getHazelcastMemberHosts(context);

            for (final String hazelcastHost : hazelcastHosts) {
                if (!niFiHosts.contains(hazelcastHost)) {
                    results.add(new ValidationResult.Builder()
                            .subject(HAZELCAST_INSTANCES.getDisplayName())
                            .valid(false)
                            .explanation("host \"" + hazelcastHost + "\" is not part of the Life cluster!")
                            .build());
                }
            }
        }

        if (!getNodeTypeProvider().getCurrentNode().isPresent() && context.getProperty(HAZELCAST_CLUSTERING_STRATEGY).getValue().equals(CLUSTER_EXPLICIT.getValue())) {
            results.add(new ValidationResult.Builder()
                    .subject(HAZELCAST_CLUSTERING_STRATEGY.getDisplayName())
                    .valid(false)
                    .explanation("cannot determine current node's host!")
                    .build());
        }

        return results;
    }
}
