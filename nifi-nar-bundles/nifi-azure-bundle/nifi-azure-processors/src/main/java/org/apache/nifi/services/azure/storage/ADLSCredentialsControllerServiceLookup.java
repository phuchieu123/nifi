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
package org.apache.nifi.services.azure.storage;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.service.lookup.AbstractSingleAttributeBasedControllerServiceLookup;

import java.util.Map;

@Tags({ "azure", "microsoft", "cloud", "storage", "adls", "credentials" })
@CapabilityDescription("Cung cấp một ADLSCredentialsService có thể được sử dụng để chọn động một ADLSCredentialsService khác. " +
    "Dịch vụ này yêu cầu một thuộc tính có tên 'adls.credentials.name' được truyền vào, và sẽ ném ra ngoại lệ nếu thuộc tính này bị thiếu. " +
    "Giá trị của 'adls.credentials.name' sẽ được dùng để chọn ADLSCredentialsService đã được đăng ký với tên đó. " +
    "Điều này cho phép định nghĩa và đăng ký nhiều ADLSCredentialsService, sau đó chọn động trong thời gian chạy bằng cách gắn thuộc tính " +
    "'adls.credentials.name' tương ứng vào flow file.")

@DynamicProperty(name = "Tên đăng ký ADLSCredentialsService", value = "ADLSCredentialsService",
    description = "Nếu thuộc tính '" + ADLSCredentialsControllerServiceLookup.ADLS_CREDENTIALS_NAME_ATTRIBUTE + "' chứa tên của thuộc tính động, " +
    "thì ADLSCredentialsService (được đăng ký trong giá trị) sẽ được chọn.",
    expressionLanguageScope = ExpressionLanguageScope.NONE)
public class ADLSCredentialsControllerServiceLookup extends AbstractSingleAttributeBasedControllerServiceLookup<ADLSCredentialsService> implements ADLSCredentialsService {

    public static final String ADLS_CREDENTIALS_NAME_ATTRIBUTE = "adls.credentials.name";

    @Override
    protected String getLookupAttribute() {
        return ADLS_CREDENTIALS_NAME_ATTRIBUTE;
    }

    @Override
    public Class<ADLSCredentialsService> getServiceType() {
        return ADLSCredentialsService.class;
    }

    @Override
    public ADLSCredentialsDetails getCredentialsDetails(final Map<String, String> attributes) {
        return lookupService(attributes).getCredentialsDetails(attributes);
    }
}
