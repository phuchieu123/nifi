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
package org.apache.nifi.services.azure;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Provides credentials used by Azure clients.
 *
 * @see AbstractControllerService
 */
@Tags({"azure", "security", "credentials", "provider", "session"})
@CapabilityDescription("Cung cấp thông tin xác thực để sử dụng với máy khách Azure.")
public class StandardAzureCredentialsControllerService extends AbstractControllerService implements AzureCredentialsService {
    public static AllowableValue DEFAULT_CREDENTIAL = new AllowableValue("default-credential",
            "Thông tin Xác thực Mặc định",
            "Sử dụng chuỗi thông tin xác thực mặc định. Trước tiên nó kiểm tra các biến môi trường, trước khi thử sử dụng danh tính được quản lý.");
    public static AllowableValue MANAGED_IDENTITY = new AllowableValue("managed-identity",
            "Danh tính được Quản lý",
            "Danh tính được Quản lý của Máy ảo Azure (chỉ có thể được sử dụng khi NiFi đang chạy trên Azure)");
    public static final PropertyDescriptor CREDENTIAL_CONFIGURATION_STRATEGY = new PropertyDescriptor.Builder()
            .name("credential-configuration-strategy")
            .displayName("Chiến lược Cấu hình Thông tin Xác thực")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .sensitive(false)
            .allowableValues(DEFAULT_CREDENTIAL, MANAGED_IDENTITY)
            .defaultValue(DEFAULT_CREDENTIAL.toString())
            .build();

    public static final PropertyDescriptor MANAGED_IDENTITY_CLIENT_ID = new PropertyDescriptor.Builder()
            .name("managed-identity-client-id")
            .displayName("ID Máy khách Danh tính được Quản lý")
            .description("ID máy khách của danh tính được quản lý. Thuộc tính được yêu cầu khi Danh tính được Quản lý do Người dùng Gán được sử dụng để xác thực. " +
                    "Nó phải để trống trong trường hợp Danh tính được Quản lý do Hệ thống Gán.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(CREDENTIAL_CONFIGURATION_STRATEGY, MANAGED_IDENTITY)
            .build();
    private static final List<PropertyDescriptor> PROPERTIES;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(CREDENTIAL_CONFIGURATION_STRATEGY);
        props.add(MANAGED_IDENTITY_CLIENT_ID);
        PROPERTIES = Collections.unmodifiableList(props);
    }

    private TokenCredential credentials;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public TokenCredential getCredentials() throws ProcessException {
        return credentials;
    }

    @OnEnabled
    public void onConfigured(final ConfigurationContext context) {
        final String configurationStrategy = context.getProperty(CREDENTIAL_CONFIGURATION_STRATEGY).getValue();

        if (DEFAULT_CREDENTIAL.equals(configurationStrategy)) {
            credentials = getDefaultAzureCredential();
        } else if (MANAGED_IDENTITY.equals(configurationStrategy)) {
            credentials = getManagedIdentityCredential(context);
        } else {
            final String errorMsg = String.format("Configuration Strategy [%s] not recognized", configurationStrategy);
            getLogger().error(errorMsg);
            throw new ProcessException(errorMsg);
        }
    }

    private TokenCredential getDefaultAzureCredential() {
        return new DefaultAzureCredentialBuilder().build();
    }

    private TokenCredential getManagedIdentityCredential(final ConfigurationContext context) {
        final String clientId = context.getProperty(MANAGED_IDENTITY_CLIENT_ID).getValue();

        final TokenCredential managedIdentityCredential = new ManagedIdentityCredentialBuilder()
                .clientId(clientId)
                .build();
        return managedIdentityCredential;
    }

    @Override
    public String toString() {
        return "StandardAzureCredentialsControllerService[id=" + getIdentifier() + "]";
    }
}
