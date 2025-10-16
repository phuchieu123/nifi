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

package org.apache.nifi.graph;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.summary.SummaryCounters;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.util.StringUtils.isEmpty;
import static org.neo4j.driver.Config.TrustStrategy.trustCustomCertificateSignedBy;

@Tags({ "graph", "neo4j", "cypher" })
@CapabilityDescription("Cung cấp một dịch vụ máy khách để quản lý các kết nối đến cơ sở dữ liệu Neo4J 4.X hoặc mới hơn. Thông tin cấu hình cho " +
        "trình điều khiển Neo4J tương ứng với hầu hết các cài đặt cho dịch vụ này có thể được tìm thấy tại đây: " +
        "https://neo4j.com/docs/driver-manual/current/client-applications/#driver-configuration. Dịch vụ này được tạo ra do " +
        "sự cố tương thích trình điều khiển giữa Neo4J 3.X và 4.X và có thể được đổi tên trong tương lai nếu và khi " +
        "Neo4J phá vỡ khả năng tương thích của trình điều khiển giữa phiên bản 4.X và một phiên bản trong tương lai.")
public class Neo4JCypherClientService extends AbstractControllerService implements GraphClientService {
    public static final PropertyDescriptor CONNECTION_URL = new PropertyDescriptor.Builder()
            .name("neo4j-connection-url")
            .displayName("URL Kết nối Neo4j")
            .description("Điểm cuối Neo4J để kết nối.")
            .required(true)
            .defaultValue("bolt://localhost:7687")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("neo4j-username")
            .displayName("Tên người dùng")
            .description("Tên người dùng để truy cập Neo4J.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("neo4j-password")
            .displayName("Mật khẩu")
            .description("Mật khẩu cho người dùng Neo4J. Yêu cầu một mật khẩu giả không trống ngay cả khi tính năng này bị vô hiệu hóa trên máy chủ.")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor CONNECTION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("neo4j-max-connection-time-out")
            .displayName("Thời gian chờ kết nối tối đa Neo4J (giây)")
            .description("Thời gian tối đa để thiết lập kết nối đến Neo4j.")
            .defaultValue("5 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor MAX_CONNECTION_POOL_SIZE = new PropertyDescriptor.Builder()
            .name("neo4j-max-connection-pool-size")
            .displayName("Kích thước vùng chứa kết nối tối đa Neo4J")
            .description("Kích thước vùng chứa kết nối tối đa cho Neo4j.")
            .defaultValue("100")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor MAX_CONNECTION_ACQUISITION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("neo4j-max-connection-acquisition-timeout")
            .displayName("Thời gian chờ lấy kết nối tối đa Neo4J")
            .description("Thời gian chờ tối đa để lấy một kết nối.")
            .defaultValue("60 second")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor IDLE_TIME_BEFORE_CONNECTION_TEST = new PropertyDescriptor.Builder()
            .name("neo4j-idle-time-before-test")
            .displayName("Thời gian chờ trước khi kiểm tra kết nối Neo4J")
            .description("Thời gian chờ (idle) trước khi thực hiện kiểm tra kết nối.")
            .defaultValue("60 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor MAX_CONNECTION_LIFETIME = new PropertyDescriptor.Builder()
            .name("neo4j-max-connection-lifetime")
            .displayName("Tuổi thọ kết nối tối đa Neo4J")
            .description("Tuổi thọ tối đa của một kết nối.")
            .defaultValue("3600 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor SSL_TRUST_STORE_FILE = new PropertyDescriptor.Builder()
            .name("SSL Trust Chain PEM")
            .description("Neo4J yêu cầu các chuỗi tin cậy phải được lưu trữ trong một tệp PEM. Nếu bạn muốn sử dụng một chuỗi tin cậy " +
                    "tùy chỉnh thay vì mặc định của hệ thống, hãy chỉ định đường dẫn đến tệp PEM chứa chuỗi tin cậy đó.")
            .required(false)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    protected Driver neo4JDriver;
    protected String username;
    protected String password;
    protected String connectionUrl;

    private static final List<PropertyDescriptor> DESCRIPTORS;
    static {
        List<PropertyDescriptor> _temp = new ArrayList<>();
        _temp.add(CONNECTION_URL);
        _temp.add(USERNAME);
        _temp.add(PASSWORD);
        _temp.add(CONNECTION_TIMEOUT);
        _temp.add(MAX_CONNECTION_POOL_SIZE);
        _temp.add(MAX_CONNECTION_ACQUISITION_TIMEOUT);
        _temp.add(IDLE_TIME_BEFORE_CONNECTION_TEST);
        _temp.add(MAX_CONNECTION_LIFETIME);
        _temp.add(SSL_TRUST_STORE_FILE);

        DESCRIPTORS = Collections.unmodifiableList(_temp);
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        String url = validationContext.getProperty(CONNECTION_URL).evaluateAttributeExpressions().getValue();
        List<ValidationResult> results = new ArrayList<>();

        if (!isEmpty(url) && (url.contains("+s://") || url.contains("+ssc://"))
            && validationContext.getProperty(SSL_TRUST_STORE_FILE).isSet()) {
            results.add(new ValidationResult.Builder()
                    .valid(false)
                    .explanation("Neo4J requires the security scheme (+s or +ssc) to not be set on the URL property " +
                            "when a custom trust chain is set. Remove the SSL modifier from the URL (ex: bolt+ssc to just 'bolt')")
                    .build());
        }

        return results;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    protected Driver getDriver(ConfigurationContext context) {
        connectionUrl = context.getProperty(CONNECTION_URL).evaluateAttributeExpressions().getValue();
        username = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
        password = context.getProperty(PASSWORD).getValue();

        Config.ConfigBuilder configBuilder = Config.builder();

        configBuilder.withMaxConnectionPoolSize(context.getProperty(MAX_CONNECTION_POOL_SIZE).evaluateAttributeExpressions().asInteger());

        configBuilder.withConnectionTimeout(context.getProperty(CONNECTION_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.SECONDS), TimeUnit.SECONDS);

        configBuilder.withConnectionAcquisitionTimeout(context.getProperty(MAX_CONNECTION_ACQUISITION_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.SECONDS), TimeUnit.SECONDS);

        configBuilder.withMaxConnectionLifetime(context.getProperty(MAX_CONNECTION_LIFETIME).evaluateAttributeExpressions().asTimePeriod(TimeUnit.SECONDS), TimeUnit.SECONDS);

        configBuilder.withConnectionLivenessCheckTimeout(context.getProperty(IDLE_TIME_BEFORE_CONNECTION_TEST).evaluateAttributeExpressions().asTimePeriod(TimeUnit.SECONDS), TimeUnit.SECONDS);

        if (context.getProperty(SSL_TRUST_STORE_FILE).isSet()) {
            String trustFile = context.getProperty(SSL_TRUST_STORE_FILE).evaluateAttributeExpressions().getValue();
            configBuilder
                    .withEncryption()
                    .withTrustStrategy(trustCustomCertificateSignedBy(new File(trustFile)));
        }

        return GraphDatabase.driver( connectionUrl, AuthTokens.basic( username, password),
                configBuilder.build());
    }

    /**
     * Helper method to help testability
     * @return Driver instance
     */
    protected Driver getNeo4JDriver() {
        return neo4JDriver;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        try {
            neo4JDriver = getDriver(context);
        } catch(Exception e) {
            getLogger().error("Error while getting connection " + e.getLocalizedMessage(),e);
            throw new ProcessException("Error while getting connection" + e.getLocalizedMessage(),e);
        }
        getLogger().info("Neo4JCypherExecutor connection created for url {}", connectionUrl);
    }

    @OnDisabled
    public void close() {
        getLogger().info("Closing driver");
        if ( neo4JDriver != null ) {
            neo4JDriver.close();
            neo4JDriver = null;
        }
    }

    private Map<String, Object> handleInternalNode(Map<String, Object> recordMap) {
        if (recordMap.size() == 1) {
            String key = recordMap.keySet().iterator().next();
            Object value = recordMap.get(key);
            if (value instanceof InternalNode) {
                return ((InternalNode)value).asMap();
            }
        }

        return recordMap;
    }

    @Override
    public Map<String, String> executeQuery(String query, Map<String, Object> parameters, GraphQueryResultCallback handler) {
        try (Session session = neo4JDriver.session()) {
            Result result = session.run(query, parameters);
            long count = 0;
            while (result.hasNext()) {
                Record record = result.next();
                Map<String, Object> asMap = handleInternalNode(record.asMap());
                handler.process(asMap, result.hasNext());
                count++;
            }

            ResultSummary summary = result.consume();
            SummaryCounters counters = summary.counters();

            Map<String,String> resultAttributes = new HashMap<>();
            resultAttributes.put(NODES_CREATED,String.valueOf(counters.nodesCreated()));
            resultAttributes.put(RELATIONS_CREATED,String.valueOf(counters.relationshipsCreated()));
            resultAttributes.put(LABELS_ADDED,String.valueOf(counters.labelsAdded()));
            resultAttributes.put(NODES_DELETED,String.valueOf(counters.nodesDeleted()));
            resultAttributes.put(RELATIONS_DELETED,String.valueOf(counters.relationshipsDeleted()));
            resultAttributes.put(PROPERTIES_SET, String.valueOf(counters.propertiesSet()));
            resultAttributes.put(ROWS_RETURNED, String.valueOf(count));

            return resultAttributes;
        } catch (Exception ex) {
            throw new ProcessException("Query execution failed", ex);
        }
    }

    @Override
    public String getTransitUrl() {
        return connectionUrl;
    }
}
