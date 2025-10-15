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

package org.apache.nifi.confluent.schemaregistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.net.ssl.SSLContext;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.confluent.schemaregistry.client.AuthenticationType;
import org.apache.nifi.confluent.schemaregistry.client.CachingSchemaRegistryClient;
import org.apache.nifi.confluent.schemaregistry.client.RestSchemaRegistryClient;
import org.apache.nifi.confluent.schemaregistry.client.SchemaRegistryClient;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.ssl.SSLContextService;

@Tags({"schema", "registry", "confluent", "avro", "kafka"})
@CapabilityDescription("Cung cấp một Schema Registry tương tác với Confluent Schema Registry để các Lược đồ được lưu trữ trong Confluent Schema Registry "
    + "có thể được sử dụng trong NiFi. Confluent Schema Registry có một khái niệm về \"chủ đề\" (subject) cho các lược đồ, đó là thuật ngữ của họ cho một tên lược đồ. Khi một Lược đồ "
    + "được tra cứu theo tên bởi registry này, nó sẽ tìm thấy một Lược đồ trong Confluent Schema Registry với chủ đề đó.")
@DynamicProperty(name = "request.header.*", value = "String literal, may not be empty", description = "Các thuộc tính bắt đầu bằng 'request.header.' " +
        "được điền vào một map và được chuyển dưới dạng tiêu đề http trong các yêu cầu REST đến Confluent Schema Registry")
public class ConfluentSchemaRegistry extends AbstractControllerService implements SchemaRegistry {

    private static final Set<SchemaField> schemaFields = EnumSet.of(SchemaField.SCHEMA_NAME, SchemaField.SCHEMA_TEXT,
        SchemaField.SCHEMA_TEXT_FORMAT, SchemaField.SCHEMA_IDENTIFIER, SchemaField.SCHEMA_VERSION);

    private static final String REQUEST_HEADER_PREFIX = "request.header.";


    static final PropertyDescriptor SCHEMA_REGISTRY_URLS = new PropertyDescriptor.Builder()
        .name("url")
        .displayName("URL của Schema Registry")
        .description("Danh sách các URL của Schema Registry được phân tách bằng dấu phẩy để tương tác")
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .defaultValue("http://localhost:8081")
        .required(true)
        .addValidator(new MultipleURLValidator())
        .build();

    static final PropertyDescriptor SSL_CONTEXT = new PropertyDescriptor.Builder()
        .name("ssl-context")
        .displayName("Dịch vụ Ngữ cảnh SSL")
        .description("Chỉ định Dịch vụ Ngữ cảnh SSL sẽ sử dụng để tương tác với Confluent Schema Registry")
        .identifiesControllerService(SSLContextService.class)
        .required(false)
        .build();

    static final PropertyDescriptor CACHE_SIZE = new PropertyDescriptor.Builder()
        .name("cache-size")
        .displayName("Kích thước Bộ đệm")
        .description("Chỉ định số lượng Lược đồ sẽ được lưu vào bộ đệm từ Schema Registry")
        .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
        .defaultValue("1000")
        .required(true)
        .build();

    static final PropertyDescriptor CACHE_EXPIRATION = new PropertyDescriptor.Builder()
        .name("cache-expiration")
        .displayName("Thời hạn Bộ đệm")
        .description("Chỉ định thời gian một Lược đồ được lưu trong bộ đệm sẽ tồn tại. Sau khi khoảng thời gian này trôi qua, một "
            + "phiên bản đã lưu trong bộ đệm của một lược đồ sẽ không còn được sử dụng, và dịch vụ sẽ phải giao tiếp với "
            + "Schema Registry một lần nữa để lấy được lược đồ.")
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .defaultValue("1 hour")
        .required(true)
        .build();

    static final PropertyDescriptor TIMEOUT = new PropertyDescriptor.Builder()
        .name("timeout")
        .displayName("Thời gian chờ Giao tiếp")
        .description("Chỉ định thời gian chờ để nhận dữ liệu từ Schema Registry trước khi coi giao tiếp là một thất bại")
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .defaultValue("30 secs")
        .required(true)
        .build();

    static final PropertyDescriptor AUTHENTICATION_TYPE = new PropertyDescriptor.Builder()
            .name("authentication-type")
            .displayName("Loại Xác thực")
            .description("Loại Xác thực Máy khách HTTP cho Confluent Schema Registry")
            .required(false)
            .allowableValues(AuthenticationType.values())
            .defaultValue(AuthenticationType.NONE.toString())
            .build();

    static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("username")
            .displayName("Tên người dùng")
            .description("Tên người dùng để xác thực với Confluent Schema Registry")
            .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("^[\\x20-\\x39\\x3b-\\x7e\\x80-\\xff]+$")))
            .required(false)
            .dependsOn(AUTHENTICATION_TYPE, AuthenticationType.BASIC.toString())
            .build();

    static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("password")
            .displayName("Mật khẩu")
            .description("Mật khẩu để xác thực với Confluent Schema Registry")
            .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("^[\\x20-\\x7e\\x80-\\xff]+$")))
            .required(false)
            .dependsOn(AUTHENTICATION_TYPE, AuthenticationType.BASIC.toString())
            .sensitive(true)
            .build();
    private volatile SchemaRegistryClient client;


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(SCHEMA_REGISTRY_URLS);
        properties.add(SSL_CONTEXT);
        properties.add(TIMEOUT);
        properties.add(CACHE_SIZE);
        properties.add(CACHE_EXPIRATION);
        properties.add(AUTHENTICATION_TYPE);
        properties.add(USERNAME);
        properties.add(PASSWORD);
        return properties;
    }

    private static final Validator REQUEST_HEADER_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String value, final ValidationContext context) {
            return new ValidationResult.Builder()
                    .subject(subject)
                    .input(value)
                    .valid(subject.startsWith(REQUEST_HEADER_PREFIX)
                            && subject.length() > REQUEST_HEADER_PREFIX.length())
                    .explanation("Dynamic property names must be of format 'request.header.*'")
                    .build();
        }
    };

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptionName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptionName)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .addValidator(REQUEST_HEADER_VALIDATOR)
                .build();
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        final List<String> baseUrls = getBaseURLs(context);
        final int timeoutMillis = context.getProperty(TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();

        final SSLContext sslContext;
        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT).asControllerService(SSLContextService.class);
        if (sslContextService == null) {
            sslContext = null;
        } else {
            sslContext = sslContextService.createContext();
        }

        final String username = context.getProperty(USERNAME).getValue();
        final String password = context.getProperty(PASSWORD).getValue();

        // generate a map of http headers where the key is the remainder of the property name after
        // the request header prefix
        final Map<String, String> httpHeaders =
                context.getProperties().entrySet()
                        .stream()
                        .filter(e -> e.getKey().getName().startsWith(REQUEST_HEADER_PREFIX))
                        .collect(Collectors.toMap(
                                map -> map.getKey().getName().substring(REQUEST_HEADER_PREFIX.length()),
                                Map.Entry::getValue)
                        );

        final SchemaRegistryClient restClient = new RestSchemaRegistryClient(baseUrls, timeoutMillis,
                sslContext, username, password, getLogger(), httpHeaders);

        final int cacheSize = context.getProperty(CACHE_SIZE).asInteger();
        final long cacheExpiration = context.getProperty(CACHE_EXPIRATION).asTimePeriod(TimeUnit.NANOSECONDS).longValue();

        client = new CachingSchemaRegistryClient(restClient, cacheSize, cacheExpiration);
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        final boolean sslContextSet = validationContext.getProperty(SSL_CONTEXT).isSet();
        if (sslContextSet) {
            final List<String> baseUrls = getBaseURLs(validationContext);
            final List<String> insecure = baseUrls.stream()
                .filter(url -> !url.startsWith("https"))
                .collect(Collectors.toList());

            if (!insecure.isEmpty()) {
                results.add(new ValidationResult.Builder()
                    .subject(SCHEMA_REGISTRY_URLS.getDisplayName())
                    .input(insecure.get(0))
                    .valid(false)
                    .explanation("When SSL Context is configured, all Schema Registry URL's must use HTTPS, not HTTP")
                    .build());
            }
        }

        final PropertyValue authenticationTypeProperty = validationContext.getProperty(AUTHENTICATION_TYPE);
        if (authenticationTypeProperty.isSet()) {
            final AuthenticationType authenticationType = AuthenticationType.valueOf(authenticationTypeProperty.getValue());
            if (AuthenticationType.BASIC.equals(authenticationType)) {
                final String username = validationContext.getProperty(USERNAME).getValue();
                if (StringUtils.isBlank(username)) {
                    results.add(new ValidationResult.Builder()
                            .subject(USERNAME.getDisplayName())
                            .valid(false)
                            .explanation("Username is required for Basic Authentication")
                            .build());
                }
                final String password = validationContext.getProperty(PASSWORD).getValue();
                if (StringUtils.isBlank(password)) {
                    results.add(new ValidationResult.Builder()
                            .subject(PASSWORD.getDisplayName())
                            .valid(false)
                            .explanation("Password is required for Basic Authentication")
                            .build());
                }
            }
        }

        return results;
    }

    private List<String> getBaseURLs(final PropertyContext context) {
        final String urls = context.getProperty(SCHEMA_REGISTRY_URLS).evaluateAttributeExpressions().getValue();
        final List<String> baseUrls = Stream.of(urls.split(","))
            .map(url -> url.trim())
            .collect(Collectors.toList());

        return baseUrls;
    }

    private RecordSchema retrieveSchemaByName(final SchemaIdentifier schemaIdentifier) throws IOException, SchemaNotFoundException {
        final Optional<String> schemaName = schemaIdentifier.getName();
        if (!schemaName.isPresent()) {
            throw new org.apache.nifi.schema.access.SchemaNotFoundException("Cannot retrieve schema because Schema Name is not present");
        }

        final RecordSchema schema;
        if (schemaIdentifier.getVersion().isPresent()) {
            schema = client.getSchema(schemaName.get(), schemaIdentifier.getVersion().getAsInt());
        } else {
            schema = client.getSchema(schemaName.get());
        }

        return schema;
    }

    private RecordSchema retrieveSchemaById(final SchemaIdentifier schemaIdentifier) throws IOException, SchemaNotFoundException {
        final OptionalLong schemaId = schemaIdentifier.getSchemaVersionId();
        if (!schemaId.isPresent()) {
            throw new org.apache.nifi.schema.access.SchemaNotFoundException("Cannot retrieve schema because Schema Id is not present");
        }

        final RecordSchema schema = client.getSchema((int) schemaId.getAsLong());
        return schema;
    }

    @Override
    public RecordSchema retrieveSchema(final SchemaIdentifier schemaIdentifier) throws IOException, SchemaNotFoundException {
        if (schemaIdentifier.getName().isPresent()) {
            return retrieveSchemaByName(schemaIdentifier);
        } else {
            return retrieveSchemaById(schemaIdentifier);
        }
    }

    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return schemaFields;
    }
}
