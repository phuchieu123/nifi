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

import java.util.Map;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.service.lookup.AbstractSingleAttributeBasedControllerServiceLookup;

@Tags({ "azure", "microsoft", "cloud", "storage", "blob", "queue", "credentials" })
@CapabilityDescription("Cung cấp một AzureStorageCredentialsService có thể được sử dụng để chọn động một AzureStorageCredentialsService khác. " +
"Dịch vụ này yêu cầu một thuộc tính có tên 'azure.storage.credentials.name' được truyền vào và sẽ ném ngoại lệ nếu thuộc tính này bị thiếu. " +
"Giá trị của thuộc tính 'azure.storage.credentials.name' sẽ được sử dụng để chọn AzureStorageCredentialsService đã được đăng ký với tên đó. " +
"Điều này cho phép định nghĩa và đăng ký nhiều AzureStorageCredentialsService khác nhau, và sau đó chọn động tại thời điểm chạy bằng cách gán cho FlowFile thuộc tính 'azure.storage.credentials.name' tương ứng.")

@DynamicProperty(
name = "Tên được dùng để đăng ký AzureStorageCredentialsService",
value = "AzureStorageCredentialsService",
description = "Nếu thuộc tính '" + AzureStorageCredentialsControllerServiceLookup.AZURE_STORAGE_CREDENTIALS_NAME_ATTRIBUTE +
"' chứa tên của thuộc tính động, thì AzureStorageCredentialsService (được đăng ký trong giá trị) sẽ được chọn.",
expressionLanguageScope = ExpressionLanguageScope.NONE
)
public class AzureStorageCredentialsControllerServiceLookup
        extends AbstractSingleAttributeBasedControllerServiceLookup<AzureStorageCredentialsService> implements AzureStorageCredentialsService {

    public static final String AZURE_STORAGE_CREDENTIALS_NAME_ATTRIBUTE = "azure.storage.credentials.name";

    @Override
    protected String getLookupAttribute() {
        return AZURE_STORAGE_CREDENTIALS_NAME_ATTRIBUTE;
    }

    @Override
    public Class<AzureStorageCredentialsService> getServiceType() {
        return AzureStorageCredentialsService.class;
    }

    @Override
    public AzureStorageCredentialsDetails getStorageCredentialsDetails(Map<String, String> attributes) {
        return lookupService(attributes).getStorageCredentialsDetails(attributes);
    }
}
