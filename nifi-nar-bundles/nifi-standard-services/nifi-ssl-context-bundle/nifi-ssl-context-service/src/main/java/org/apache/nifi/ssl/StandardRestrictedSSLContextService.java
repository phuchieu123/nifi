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
package org.apache.nifi.ssl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.security.util.TlsPlatform;

/**
 * This class is functionally the same as {@link StandardSSLContextService}, but it restricts the allowable
 * values that can be selected for TLS/SSL protocols.
 */
@Tags({"tls", "ssl", "secure", "certificate", "keystore", "truststore", "jks", "p12", "pkcs12", "pkcs"})
@CapabilityDescription("Triển khai Bị hạn chế của SSLContextService. Cung cấp khả năng cấu hình "
        + "các thuộc tính keystore và/hoặc truststore một lần và tái sử dụng cấu hình đó trong suốt ứng dụng, "
        + "nhưng chỉ cho phép chọn một bộ giao thức TLS/SSL bị hạn chế (không hỗ trợ các giao thức SSL). Bộ giao thức có thể chọn sẽ "
        + "phát triển theo thời gian khi các giao thức mới xuất hiện và các giao thức cũ hơn bị loại bỏ. Dịch vụ này được khuyến nghị "
        + "so với StandardSSLContextService nếu một thành phần không dự kiến giao tiếp với các hệ thống cũ vì nó là "
        + "không có khả năng là các hệ thống cũ sẽ hỗ trợ các giao thức này.")
public class StandardRestrictedSSLContextService extends StandardSSLContextService implements RestrictedSSLContextService {

    public static final PropertyDescriptor RESTRICTED_SSL_ALGORITHM = new PropertyDescriptor.Builder()
            .name("Giao thức SSL")
            .displayName("Giao thức TLS")
            .defaultValue(TlsConfiguration.TLS_PROTOCOL)
            .required(false)
            .allowableValues(getRestrictedProtocolAllowableValues())
            .description("Phiên bản Giao thức TLS cho các kết nối được mã hóa. Các phiên bản được hỗ trợ phụ thuộc vào phiên bản Java cụ thể được sử dụng.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(false)
            .build();

    private static final List<PropertyDescriptor> properties;

    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(KEYSTORE);
        props.add(KEYSTORE_PASSWORD);
        props.add(KEY_PASSWORD);
        props.add(KEYSTORE_TYPE);
        props.add(TRUSTSTORE);
        props.add(TRUSTSTORE_PASSWORD);
        props.add(TRUSTSTORE_TYPE);
        props.add(RESTRICTED_SSL_ALGORITHM);
        properties = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public String getSslAlgorithm() {
        return configContext.getProperty(RESTRICTED_SSL_ALGORITHM).getValue();
    }

    private static AllowableValue[] getRestrictedProtocolAllowableValues() {
        final List<AllowableValue> allowableValues = new ArrayList<>();

        allowableValues.add(new AllowableValue(TlsConfiguration.TLS_PROTOCOL, TlsConfiguration.TLS_PROTOCOL, "Negotiate latest protocol version based on platform supported versions"));

        for (final String preferredProtocol : TlsPlatform.getPreferredProtocols()) {
            final String description = String.format("Require %s protocol version", preferredProtocol);
            allowableValues.add(new AllowableValue(preferredProtocol, preferredProtocol, description));
        }

        return allowableValues.toArray(new AllowableValue[allowableValues.size()]);
    }
}
