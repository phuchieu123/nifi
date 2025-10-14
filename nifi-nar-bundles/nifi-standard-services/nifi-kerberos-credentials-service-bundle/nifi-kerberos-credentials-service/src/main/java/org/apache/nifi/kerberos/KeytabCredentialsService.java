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

package org.apache.nifi.kerberos;

import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@CapabilityDescription("Cung cấp cơ chế chỉ định Keytab và Principal để các component khác có thể sử dụng cho việc "
        + "xác thực Kerberos. Bằng cách đóng gói thông tin này vào Controller Service và cho phép các component khác sử dụng "
        + "(thay vì chỉ định trực tiếp principal và keytab trong processor), quản trị viên có thể kiểm soát người dùng nào "
        + "được phép sử dụng keytab và principal nào. Điều này cung cấp mô hình bảo mật mạnh mẽ hơn cho môi trường đa tenant.")
@Tags({"Kerberos", "Keytab", "Principal", "Credentials", "Authentication", "Security"})
@Restricted(restrictions = {
    @Restriction(requiredPermission = RequiredPermission.ACCESS_KEYTAB, explanation = "Cho phép người dùng định nghĩa Keytab và principal, sau đó có thể được các component khác sử dụng.")
})
public class KeytabCredentialsService extends AbstractControllerService implements KerberosCredentialsService {

    static final PropertyDescriptor PRINCIPAL = new PropertyDescriptor.Builder()
        .name("Kerberos Principal")
        .description("Kerberos principal để xác thực. Yêu cầu nifi.kerberos.krb5.file được thiết lập trong nifi.properties")
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .required(true)
        .build();

    static final PropertyDescriptor KEYTAB = new PropertyDescriptor.Builder()
        .name("Kerberos Keytab")
        .description("Keytab Kerberos liên kết với principal. Yêu cầu nifi.kerberos.krb5.file được thiết lập trong nifi.properties")
        .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .required(true)
        .build();

    private File kerberosConfigFile;
    private volatile String principal;
    private volatile String keytab;

    @Override
    protected final void init(final ControllerServiceInitializationContext config) throws InitializationException {
        kerberosConfigFile = config.getKerberosConfigurationFile();
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        // Kiểm tra Kerberos configuration
        if (kerberosConfigFile == null) {
            results.add(new ValidationResult.Builder()
                .subject("Kerberos Configuration File")
                .valid(false)
                .explanation("Cần thiết lập nifi.kerberos.krb5.file trong nifi.properties để sử dụng xác thực Kerberos")
                .build());
        } else if (!kerberosConfigFile.canRead()) {
            // Kiểm tra quyền đọc file
            results.add(new ValidationResult.Builder()
                .subject("Kerberos Configuration File")
                .valid(false)
                .explanation("Không thể đọc file Kerberos Configuration File " + kerberosConfigFile.getAbsolutePath()
                    + " được chỉ định trong nifi.properties. Vui lòng đảm bảo đường dẫn hợp lệ và NiFi có quyền đọc file.")
                .build());
        }

        return results;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(2);
        properties.add(KEYTAB);
        properties.add(PRINCIPAL);
        return properties;
    }

    @OnEnabled
    public void setConfiguredValues(final ConfigurationContext context) {
        this.keytab = context.getProperty(KEYTAB).evaluateAttributeExpressions().getValue();
        this.principal = context.getProperty(PRINCIPAL).evaluateAttributeExpressions().getValue();
    }

    @Override
    public String getKeytab() {
        return keytab;
    }

    @Override
    public String getPrincipal() {
        return principal;
    }
}