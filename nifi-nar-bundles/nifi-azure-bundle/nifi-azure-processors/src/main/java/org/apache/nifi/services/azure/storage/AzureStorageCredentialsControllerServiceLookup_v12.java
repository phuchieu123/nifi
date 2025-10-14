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

@Tags({ "azure", "microsoft", "cloud", "storage", "blob", "queue", "credentials" })
@CapabilityDescription("Cung cấp một AzureStorageCredentialsService_v12 có thể được sử dụng để chọn động một AzureStorageCredentialsService_v12 khác. " +
"Dịch vụ này yêu cầu một thuộc tính có tên 'azure.storage.credentials.name' được truyền vào, và sẽ gây lỗi (ném ngoại lệ) nếu thuộc tính này bị thiếu. " +
"Giá trị của thuộc tính 'azure.storage.credentials.name' sẽ được sử dụng để chọn AzureStorageCredentialsService_v12 đã được đăng ký với tên đó. " +
"Điều này cho phép định nghĩa và đăng ký nhiều AzureStorageCredentialsService_v12 khác nhau, và sau đó chọn động tại thời điểm chạy bằng cách gán thuộc tính 'azure.storage.credentials.name' tương ứng cho FlowFile.")

@DynamicProperty(
name = "Tên được dùng để đăng ký AzureStorageCredentialsService_v12",
value = "AzureStorageCredentialsService_v12",
description = "Nếu thuộc tính '" + AzureStorageCredentialsControllerServiceLookup_v12.AZURE_STORAGE_CREDENTIALS_NAME_ATTRIBUTE +
"' chứa tên của thuộc tính động, thì AzureStorageCredentialsService_v12 (được đăng ký trong giá trị) sẽ được chọn."
)
public class AzureStorageCredentialsControllerServiceLookup_v12
        extends AbstractSingleAttributeBasedControllerServiceLookup<AzureStorageCredentialsService_v12> implements AzureStorageCredentialsService_v12 {

    public static final String AZURE_STORAGE_CREDENTIALS_NAME_ATTRIBUTE = "azure.storage.credentials.name";

    @Override
    protected String getLookupAttribute() {
        return AZURE_STORAGE_CREDENTIALS_NAME_ATTRIBUTE;
    }

    @Override
    public Class<AzureStorageCredentialsService_v12> getServiceType() {
        return AzureStorageCredentialsService_v12.class;
    }

    @Override
    public AzureStorageCredentialsDetails_v12 getCredentialsDetails(final Map<String, String> attributes) {
        return lookupService(attributes).getCredentialsDetails(attributes);
    }
}
