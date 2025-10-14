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
package org.apache.nifi.dbcp;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.SupportsSensitiveDynamicProperties;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.security.krb.KerberosAction;
import org.apache.nifi.security.krb.KerberosUser;

import javax.security.auth.login.LoginException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Implementation of Database Connection Pooling Service. HikariCP is used for connection pooling functionality.
 */
@RequiresInstanceClassLoading
@Tags({"dbcp", "hikari", "jdbc", "database", "connection", "pooling", "store"})
@CapabilityDescription("Cung cấp dịch vụ quản lý kết nối cơ sở dữ liệu dựa trên HikariCP. Kết nối có thể được mượn từ pool và trả lại sau khi sử dụng.")
@SupportsSensitiveDynamicProperties
@DynamicProperty(name = "Tên thuộc tính JDBC", value = "Giá trị thuộc tính JDBC", expressionLanguageScope = ExpressionLanguageScope.VARIABLE_REGISTRY,
        description = "Chỉ định tên thuộc tính và giá trị được thiết lập trên kết nối JDBC. "
                + "Nếu sử dụng Expression Language, việc đánh giá sẽ thực hiện khi controller service được kích hoạt. "
                + "Lưu ý rằng không có dữ liệu flow file (attributes, v.v.) được sử dụng trong các biểu thức này.")
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.REFERENCE_REMOTE_RESOURCES,
                        explanation = "Vị trí Driver cơ sở dữ liệu có thể tham chiếu tài nguyên qua HTTP"
                )
        }
)
public class HikariCPConnectionPool extends AbstractControllerService implements DBCPService {
    /**
     * Prefix cho các thuộc tính động nhạy cảm
     */
    protected static final String SENSITIVE_PROPERTY_PREFIX = "SENSITIVE.";
    protected static final long INFINITE_MILLISECONDS = -1L;

    private static final String DEFAULT_TOTAL_CONNECTIONS = "10";
    private static final String DEFAULT_MAX_CONN_LIFETIME = "-1";

    public static final PropertyDescriptor DATABASE_URL = new PropertyDescriptor.Builder()
            .name("hikaricp-connection-url")
            .displayName("URL Kết nối Cơ sở dữ liệu")
            .description("URL kết nối cơ sở dữ liệu được sử dụng để kết nối đến cơ sở dữ liệu. Có thể bao gồm tên hệ thống cơ sở dữ liệu, host, port, tên cơ sở dữ liệu và một số tham số. "
                    + "Cú pháp chính xác được xác định bởi hệ quản trị cơ sở dữ liệu của bạn.")
            .defaultValue(null)
            .addValidator(new ConnectionUrlValidator())
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor DB_DRIVERNAME = new PropertyDescriptor.Builder()
            .name("hikaricp-driver-classname")
            .displayName("Tên lớp Driver Cơ sở dữ liệu")
            .description("Tên lớp đầy đủ của driver JDBC. Ví dụ: com.mysql.jdbc.Driver")
            .defaultValue(null)
            .required(true)
            .addValidator(new DriverClassValidator())
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor DB_DRIVER_LOCATION = new PropertyDescriptor.Builder()
            .name("hikaricp-driver-locations")
            .displayName("Vị trí Driver Cơ sở dữ liệu")
            .description("Danh sách các file/thư mục và/hoặc URL, cách nhau bằng dấu phẩy, chứa file JAR của driver và các thư viện phụ thuộc (nếu có). Ví dụ: '/var/tmp/mariadb-java-client-1.1.7.jar'")
            .defaultValue(null)
            .required(false)
            .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE, ResourceType.DIRECTORY, ResourceType.URL)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .dynamicallyModifiesClasspath(true)
            .build();

    public static final PropertyDescriptor DB_USER = new PropertyDescriptor.Builder()
            .name("hikaricp-username")
            .displayName("Người dùng cơ sở dữ liệu")
            .description("Tên người dùng cơ sở dữ liệu")
            .defaultValue(null)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor DB_PASSWORD = new PropertyDescriptor.Builder()
            .name("hikaricp-password")
            .displayName("Mật khẩu")
            .description("Mật khẩu cho người dùng cơ sở dữ liệu")
            .defaultValue(null)
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor MAX_WAIT_TIME = new PropertyDescriptor.Builder()
            .name("hikaricp-max-wait-time")
            .displayName("Thời gian chờ tối đa")
            .description("Thời gian tối đa mà pool sẽ chờ (khi không có kết nối sẵn có) để một kết nối được trả lại trước khi thất bại, hoặc 0 <đơn vị thời gian> để chờ vô thời hạn.")
            .defaultValue("500 millis")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .sensitive(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor MAX_TOTAL_CONNECTIONS = new PropertyDescriptor.Builder()
            .name("hikaricp-max-total-conns")
            .displayName("Số kết nối tối đa")
            .description("Thuộc tính này kiểm soát kích thước tối đa mà pool được phép đạt tới, bao gồm cả kết nối rảnh và đang sử dụng. Khi pool đạt tới kích thước này và không có kết nối rảnh, dịch vụ sẽ chặn tối đa connectionTimeout milliseconds trước khi timeout.")
            .defaultValue(DEFAULT_TOTAL_CONNECTIONS)
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .sensitive(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor VALIDATION_QUERY = new PropertyDescriptor.Builder()
            .name("hikaricp-validation-query")
            .displayName("Câu truy vấn kiểm tra kết nối")
            .description("Câu truy vấn dùng để kiểm tra kết nối trước khi trả lại chúng. Khi kết nối không hợp lệ, nó sẽ bị hủy và kết nối hợp lệ mới sẽ được trả lại. "
                    + "LƯU Ý: Sử dụng kiểm tra kết nối có thể gây ảnh hưởng hiệu năng.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor MIN_IDLE = new PropertyDescriptor.Builder()
            .name("hikaricp-min-idle-conns")
            .displayName("Số kết nối rảnh tối thiểu")
            .description("Thuộc tính này kiểm soát số kết nối rảnh tối thiểu mà HikariCP cố gắng duy trì trong pool. Nếu số kết nối rảnh giảm xuống dưới giá trị này và tổng số kết nối trong pool nhỏ hơn 'Số kết nối tối đa', HikariCP sẽ cố gắng thêm kết nối nhanh và hiệu quả. Nên thiết lập bằng 'Số kết nối tối đa'.")
            .defaultValue(DEFAULT_TOTAL_CONNECTIONS)
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor MAX_CONN_LIFETIME = new PropertyDescriptor.Builder()
            .name("hikaricp-max-conn-lifetime")
            .displayName("Tuổi thọ kết nối tối đa")
            .description("Tuổi thọ tối đa của một kết nối. Sau thời gian này, kết nối sẽ thất bại khi kích hoạt, đưa vào pool hoặc kiểm tra. Giá trị bằng 0 hoặc nhỏ hơn có nghĩa là kết nối có tuổi thọ vô hạn.")
            .defaultValue(DEFAULT_MAX_CONN_LIFETIME)
            .required(false)
            .addValidator(DBCPValidator.CUSTOM_TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor KERBEROS_USER_SERVICE = new PropertyDescriptor.Builder()
            .name("hikaricp-kerberos-user-service")
            .displayName("Dịch vụ Người dùng Kerberos")
            .description("Chỉ định Dịch vụ Controller Người dùng Kerberos sẽ được sử dụng để xác thực với Kerberos")
            .identifiesControllerService(KerberosUserService.class)
            .required(false)
            .build();


    private static final List<PropertyDescriptor> properties;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(DATABASE_URL);
        props.add(DB_DRIVERNAME);
        props.add(DB_DRIVER_LOCATION);
        props.add(KERBEROS_USER_SERVICE);
        props.add(DB_USER);
        props.add(DB_PASSWORD);
        props.add(MAX_WAIT_TIME);
        props.add(MAX_TOTAL_CONNECTIONS);
        props.add(VALIDATION_QUERY);
        props.add(MIN_IDLE);
        props.add(MAX_CONN_LIFETIME);

        properties = Collections.unmodifiableList(props);
    }

    private volatile HikariDataSource dataSource;
    private volatile KerberosUser kerberosUser;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        final PropertyDescriptor.Builder builder = new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .dynamic(true)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR);

        if (propertyDescriptorName.startsWith(SENSITIVE_PROPERTY_PREFIX)) {
            builder.sensitive(true).expressionLanguageSupported(ExpressionLanguageScope.NONE);
        } else {
            builder.expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY);
        }

        return builder.build();
    }

    /**
     * Configures connection pool by creating an instance of the
     * {@link HikariDataSource} based on configuration provided with
     * {@link ConfigurationContext}.
     * <p>
     * This operation makes no guarantees that the actual connection could be
     * made since the underlying system may still go off-line during normal
     * operation of the connection pool.
     *
     * @param context the configuration context
     */
    @OnEnabled
    public void onConfigured(final ConfigurationContext context) {

        final String driverName = context.getProperty(DB_DRIVERNAME).evaluateAttributeExpressions().getValue();
        final String user = context.getProperty(DB_USER).evaluateAttributeExpressions().getValue();
        final String passw = context.getProperty(DB_PASSWORD).evaluateAttributeExpressions().getValue();
        final String dburl = context.getProperty(DATABASE_URL).evaluateAttributeExpressions().getValue();
        final Integer maxTotal = context.getProperty(MAX_TOTAL_CONNECTIONS).evaluateAttributeExpressions().asInteger();
        final String validationQuery = context.getProperty(VALIDATION_QUERY).evaluateAttributeExpressions().getValue();
        final long maxWaitMillis = extractMillisWithInfinite(context.getProperty(MAX_WAIT_TIME).evaluateAttributeExpressions());
        final int minIdle = context.getProperty(MIN_IDLE).evaluateAttributeExpressions().asInteger();
        final long maxConnLifetimeMillis = extractMillisWithInfinite(context.getProperty(MAX_CONN_LIFETIME).evaluateAttributeExpressions());
        final KerberosUserService kerberosUserService = context.getProperty(KERBEROS_USER_SERVICE).asControllerService(KerberosUserService.class);

        if (kerberosUserService != null) {
            kerberosUser = kerberosUserService.createKerberosUser();
            if (kerberosUser != null) {
                kerberosUser.login();
            }
        }

        dataSource = new HikariDataSource();
        dataSource.setDriverClassName(driverName);
        dataSource.setConnectionTimeout(maxWaitMillis);
        dataSource.setMaximumPoolSize(maxTotal);
        dataSource.setMinimumIdle(minIdle);
        dataSource.setMaxLifetime(maxConnLifetimeMillis);

        if (validationQuery != null && !validationQuery.isEmpty()) {
            dataSource.setConnectionTestQuery(validationQuery);
        }

        dataSource.setJdbcUrl(dburl);
        dataSource.setUsername(user);
        dataSource.setPassword(passw);

        final List<PropertyDescriptor> dynamicProperties = context.getProperties()
                .keySet()
                .stream()
                .filter(PropertyDescriptor::isDynamic)
                .collect(Collectors.toList());

        Properties properties = dataSource.getDataSourceProperties();
        dynamicProperties.forEach((descriptor) -> {
            final PropertyValue propertyValue = context.getProperty(descriptor);
            if (descriptor.isSensitive()) {
                final String propertyName = StringUtils.substringAfter(descriptor.getName(), SENSITIVE_PROPERTY_PREFIX);
                properties.setProperty(propertyName, propertyValue.getValue());
            } else {
                properties.setProperty(descriptor.getName(), propertyValue.evaluateAttributeExpressions().getValue());
            }
        });
        dataSource.setDataSourceProperties(properties);
        dataSource.setPoolName(toString());
    }

    private long extractMillisWithInfinite(PropertyValue prop) {
        return "-1".equals(prop.getValue()) ? INFINITE_MILLISECONDS : prop.asTimePeriod(TimeUnit.MILLISECONDS);
    }

    /**
     * Shutdown pool, close all open connections.
     * If a principal is authenticated with a KDC, that principal is logged out.
     * <p>
     * If a @{@link LoginException} occurs while attempting to log out the @{@link org.apache.nifi.security.krb.KerberosUser},
     * an attempt will still be made to shut down the pool and close open connections.
     *
     */
    @OnDisabled
    public void shutdown() {
        try {
            if (kerberosUser != null) {
                kerberosUser.logout();
            }
        } finally {
            kerberosUser = null;
            try {
                if (dataSource != null) {
                    dataSource.close();
                }
            } finally {
                dataSource = null;
            }
        }
    }

    @Override
    public Connection getConnection() throws ProcessException {
        try {
            final Connection con;
            if (kerberosUser != null) {
                KerberosAction<Connection> kerberosAction = new KerberosAction<>(kerberosUser, () -> dataSource.getConnection(), getLogger());
                con = kerberosAction.execute();
            } else {
                con = dataSource.getConnection();
            }
            return con;
        } catch (final SQLException e) {
            // If using Kerberos,  attempt to re-login
            if (kerberosUser != null) {
                getLogger().info("Error getting connection, performing Kerberos re-login");
                kerberosUser.login();
            }
            throw new ProcessException("Connection retrieval failed", e);
        }
    }

    @Override
    public String toString() {
        return String.format("%s[id=%s]", getClass().getSimpleName(), getIdentifier());
    }

    HikariDataSource getDataSource() {
        return dataSource;
    }
}
