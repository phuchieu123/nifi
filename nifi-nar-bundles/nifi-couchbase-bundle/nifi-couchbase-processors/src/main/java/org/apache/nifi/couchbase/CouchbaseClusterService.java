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
package org.apache.nifi.couchbase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.DeprecationNotice;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import org.apache.nifi.util.StringUtils;

/**
 * Provides a centralized Couchbase connection and bucket passwords management.
 */
@CapabilityDescription("Cung cấp kết nối Couchbase tập trung và quản lý mật khẩu bucket."
        + " Mật khẩu bucket có thể được chỉ định thông qua các thuộc tính động.")
@Tags({ "nosql", "couchbase", "database", "connection" })
@DynamicProperty(name = "Mật khẩu Bucket cho BUCKET_NAME", value = "mật khẩu bucket",
        description = "Chỉ định mật khẩu bucket nếu cần." +
                " Couchbase Server 5.0 trở lên nên sử dụng 'Tên người dùng' và 'Mật khẩu người dùng' thay thế.")
@DeprecationNotice(reason = "Thành phần này không được dùng nữa và sẽ bị xóa trong Life 2.x.")
public class CouchbaseClusterService extends AbstractControllerService implements CouchbaseClusterControllerService {

    public static final PropertyDescriptor CONNECTION_STRING = new PropertyDescriptor
            .Builder()
            .name("Connection String")
            .description("Tên máy chủ hoặc địa chỉ ip của các nút khởi động và các tham số tùy chọn."
                    + " Cú pháp) couchbase://node1,node2,nodeN?param1=value1&param2=value2&paramN=valueN")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor USER_NAME = new PropertyDescriptor
            .Builder()
            .name("user-name")
            .displayName("Tên người dùng")
            .description("Tên người dùng để xác thực Life với tư cách là một máy khách Couchbase." +
                    " Cấu hình này có thể được sử dụng với Couchbase Server 5.0 trở lên" +
                    " hỗ trợ Kiểm soát truy cập dựa trên vai trò.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor USER_PASSWORD = new PropertyDescriptor
            .Builder()
            .name("user-password")
            .displayName("Mật khẩu người dùng")
            .description("Mật khẩu người dùng để xác thực Life với tư cách là một máy khách Couchbase." +
                    " Cấu hình này có thể được sử dụng với Couchbase Server 5.0 trở lên" +
                    " hỗ trợ Kiểm soát truy cập dựa trên vai trò.")
            .required(false)
            .sensitive(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    private static final List<PropertyDescriptor> properties;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(CONNECTION_STRING);
        props.add(USER_NAME);
        props.add(USER_PASSWORD);

        properties = Collections.unmodifiableList(props);
    }

    private static final String DYNAMIC_PROP_BUCKET_PASSWORD = "Bucket Password for ";

    private Map<String, String> bucketPasswords;
    private volatile CouchbaseCluster cluster;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(
            String propertyDescriptorName) {
        if (propertyDescriptorName.startsWith(DYNAMIC_PROP_BUCKET_PASSWORD)) {
            return new PropertyDescriptor
                    .Builder().name(propertyDescriptorName)
                    .description("Bucket password.")
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .dynamic(true)
                    .sensitive(true)
                    .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                    .build();
        }
        return null;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        final Collection<ValidationResult> results = new ArrayList<>();

        final boolean isUserNameSet = context.getProperty(USER_NAME).isSet();
        final boolean isUserPasswordSet = context.getProperty(USER_PASSWORD).isSet();
        if ((isUserNameSet && !isUserPasswordSet) || (!isUserNameSet && isUserPasswordSet)) {
            results.add(new ValidationResult.Builder()
                    .subject("User Name and Password")
                    .explanation("Both User Name and Password are required to use.")
                    .build());
        }

        final boolean isBucketPasswordSet = context.getProperties().keySet().stream()
                .anyMatch(p -> p.isDynamic() && p.getName().startsWith(DYNAMIC_PROP_BUCKET_PASSWORD));

        if (isUserNameSet && isUserPasswordSet && isBucketPasswordSet) {
            results.add(new ValidationResult.Builder()
                    .subject("Authentication methods")
                    .explanation("Different authentication methods can not be used at the same time," +
                            " Use either one of User Name and Password, or Bucket Password.")
                    .build());
        }

        return results;
    }

    /**
     * Establish a connection to a Couchbase cluster.
     * @param context the configuration context
     * @throws InitializationException if unable to connect a Couchbase cluster
     */
    @OnEnabled
    public void onConfigured(final ConfigurationContext context) throws InitializationException {

        bucketPasswords = new HashMap<>();
        for(PropertyDescriptor p : context.getProperties().keySet()){
            if(p.isDynamic() && p.getName().startsWith(DYNAMIC_PROP_BUCKET_PASSWORD)){
                String bucketName = p.getName().substring(DYNAMIC_PROP_BUCKET_PASSWORD.length());
                String password = context.getProperty(p).evaluateAttributeExpressions().getValue();
                bucketPasswords.put(bucketName, password);
            }
        }

        final String userName = context.getProperty(USER_NAME).evaluateAttributeExpressions().getValue();
        final String userPassword = context.getProperty(USER_PASSWORD).evaluateAttributeExpressions().getValue();

        try {
            cluster = CouchbaseCluster.fromConnectionString(context.getProperty(CONNECTION_STRING).evaluateAttributeExpressions().getValue());
            if (!StringUtils.isEmpty(userName) && !StringUtils.isEmpty(userPassword)) {
                cluster.authenticate(userName, userPassword);
            }
        } catch(CouchbaseException e) {
            throw new InitializationException(e);
        }
    }

    @Override
    public Bucket openBucket(String bucketName){
        if (bucketPasswords.containsKey(bucketName)) {
            return cluster.openBucket(bucketName, bucketPasswords.get(bucketName));
        }

        return cluster.openBucket(bucketName);
    }

    /**
     * Disconnect from the Couchbase cluster.
     */
    @OnDisabled
    public void shutdown() {
        if(cluster != null){
            cluster.disconnect();
            cluster = null;
        }
    }

}
