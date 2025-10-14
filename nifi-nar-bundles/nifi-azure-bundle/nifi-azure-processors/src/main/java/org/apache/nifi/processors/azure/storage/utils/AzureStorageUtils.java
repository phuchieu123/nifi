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
package org.apache.nifi.processors.azure.storage.utils;

import com.azure.core.http.ProxyOptions;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;
import com.microsoft.azure.storage.StorageCredentialsSharedAccessSignature;
import com.microsoft.azure.storage.blob.CloudBlobClient;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxySpec;
import org.apache.nifi.proxy.SocksVersion;
import org.apache.nifi.services.azure.storage.AzureStorageCredentialsDetails;
import org.apache.nifi.services.azure.storage.AzureStorageCredentialsService;
import org.apache.nifi.services.azure.storage.AzureStorageEmulatorCredentialsDetails;
import reactor.netty.http.client.HttpClient;

public final class AzureStorageUtils {
    public static final String BLOCK = "Block";
    public static final String PAGE = "Page";

    public static final String STORAGE_ACCOUNT_NAME_PROPERTY_DESCRIPTOR_NAME = "storage-account-name";
    public static final String STORAGE_ACCOUNT_KEY_PROPERTY_DESCRIPTOR_NAME = "storage-account-key";
    public static final String STORAGE_SAS_TOKEN_PROPERTY_DESCRIPTOR_NAME = "storage-sas-token";
    public static final String STORAGE_ENDPOINT_SUFFIX_PROPERTY_DESCRIPTOR_NAME = "storage-endpoint-suffix";

    public static final String ACCOUNT_KEY_BASE_DESCRIPTION =
            "Khóa tài khoản lưu trữ. Đây là mật khẩu có quyền quản trị, cho phép truy cập vào mọi vùng chứa (container) trong tài khoản này. " +
            "Khuyến nghị nên sử dụng mã chia sẻ truy cập (SAS token) thay thế để có khả năng kiểm soát chi tiết hơn với chính sách riêng.";

    public static final String ACCOUNT_KEY_SECURITY_DESCRIPTION =
            " Có một số rủi ro khi cho phép lưu khóa tài khoản trong thuộc tính của FlowFile. " +
            "Mặc dù điều này giúp luồng xử lý linh hoạt hơn khi có thể lấy khóa tài khoản động từ thuộc tính của FlowFile, " +
            "nhưng cần đảm bảo giới hạn quyền truy cập dữ liệu nguồn (provenance), ví dụ bằng cách kiểm soát chặt chẽ các chính sách truy cập. " +
            "Ngoài ra, nên đặt các kho lưu trữ provenance trên phân vùng đĩa được mã hóa.";

    public static final PropertyDescriptor ACCOUNT_KEY = new PropertyDescriptor.Builder()
            .name(STORAGE_ACCOUNT_KEY_PROPERTY_DESCRIPTOR_NAME)
            .displayName("Khóa tài khoản lưu trữ")
            .description(ACCOUNT_KEY_BASE_DESCRIPTION + ACCOUNT_KEY_SECURITY_DESCRIPTION)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .sensitive(true)
            .build();

    public static final String ACCOUNT_NAME_BASE_DESCRIPTION = "Tên tài khoản lưu trữ.";

    public static final String ACCOUNT_NAME_SECURITY_DESCRIPTION =
            " Có một số rủi ro khi cho phép lưu tên tài khoản trong thuộc tính của FlowFile. " +
            "Mặc dù điều này giúp luồng xử lý linh hoạt hơn khi có thể lấy tên tài khoản động từ thuộc tính của FlowFile, " +
            "nhưng cần đảm bảo giới hạn quyền truy cập dữ liệu nguồn (provenance), ví dụ bằng cách kiểm soát chặt chẽ các chính sách truy cập. " +
            "Ngoài ra, nên đặt các kho lưu trữ provenance trên phân vùng đĩa được mã hóa.";

    public static final String ACCOUNT_NAME_CREDENTIAL_SERVICE_DESCRIPTION =
            " Thay vì khai báo trực tiếp các thuộc tính Tên tài khoản, Khóa tài khoản và Mã SAS trong bộ xử lý, " +
            "cách được khuyến nghị là cấu hình chúng thông qua một dịch vụ điều khiển (controller service) được chỉ định trong thuộc tính Thông tin xác thực lưu trữ. " +
            "Dịch vụ này có thể cung cấp cấu hình dùng chung cho nhiều bộ xử lý Azure. Ngoài ra, phiên bản 'Tra cứu' của dịch vụ cũng có thể cho phép lấy thông tin xác thực động theo thuộc tính FlowFile.";

    public static final PropertyDescriptor ACCOUNT_NAME = new PropertyDescriptor.Builder()
            .name(STORAGE_ACCOUNT_NAME_PROPERTY_DESCRIPTOR_NAME)
            .displayName("Tên tài khoản lưu trữ")
            .description(ACCOUNT_NAME_BASE_DESCRIPTION + ACCOUNT_NAME_SECURITY_DESCRIPTION + ACCOUNT_NAME_CREDENTIAL_SERVICE_DESCRIPTION)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor ENDPOINT_SUFFIX = new PropertyDescriptor.Builder()
            .name(STORAGE_ENDPOINT_SUFFIX_PROPERTY_DESCRIPTOR_NAME)
            .displayName("Hậu tố điểm cuối tài khoản lưu trữ chung")
            .description(
                    "Các tài khoản lưu trữ trong Azure công cộng luôn sử dụng hậu tố FQDN chung. " +
                    "Có thể ghi đè hậu tố này trong một số trường hợp đặc biệt (như Azure Stack hoặc vùng Azure không công khai). " +
                    "Cách khuyến nghị là cấu hình qua dịch vụ điều khiển được chỉ định trong thuộc tính Thông tin xác thực lưu trữ. " +
                    "Dịch vụ này có thể cung cấp cấu hình dùng chung cho nhiều bộ xử lý Azure. Ngoài ra, phiên bản 'Tra cứu' của dịch vụ cũng cho phép chọn thông tin xác thực động khi chạy.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor CONTAINER = new PropertyDescriptor.Builder()
            .name("container-name")
            .displayName("Tên vùng chứa (Container)")
            .description("Tên vùng chứa lưu trữ Azure. Trong trường hợp của bộ xử lý PutAzureBlobStorage, vùng chứa sẽ được tự động tạo nếu chưa tồn tại.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    public static final String SAS_TOKEN_BASE_DESCRIPTION = "Mã chia sẻ truy cập (Shared Access Signature - SAS), bao gồm cả ký tự '?' ở đầu. Chỉ định hoặc mã SAS (được khuyến nghị) hoặc Khóa tài khoản.";

    public static final String SAS_TOKEN_SECURITY_DESCRIPTION =
            " Có một số rủi ro khi cho phép lưu mã SAS trong thuộc tính của FlowFile. " +
            "Mặc dù điều này giúp luồng xử lý linh hoạt hơn khi có thể lấy mã SAS động từ thuộc tính của FlowFile, " +
            "nhưng cần đảm bảo giới hạn quyền truy cập dữ liệu nguồn (provenance), ví dụ bằng cách kiểm soát chặt chẽ các chính sách truy cập. " +
            "Ngoài ra, nên đặt các kho lưu trữ provenance trên phân vùng đĩa được mã hóa.";

    public static final PropertyDescriptor PROP_SAS_TOKEN = new PropertyDescriptor.Builder()
            .name(STORAGE_SAS_TOKEN_PROPERTY_DESCRIPTOR_NAME)
            .displayName("Mã SAS")
            .description(SAS_TOKEN_BASE_DESCRIPTION + SAS_TOKEN_SECURITY_DESCRIPTION)
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor STORAGE_CREDENTIALS_SERVICE = new PropertyDescriptor.Builder()
            .name("storage-credentials-service")
            .displayName("Thông tin xác thực lưu trữ")
            .description("Dịch vụ điều khiển (Controller Service) được sử dụng để lấy thông tin xác thực Azure Storage. " +
                    "Thay vì khai báo trực tiếp trong bộ xử lý, thông tin xác thực nên được cấu hình tại đây để dùng chung giữa nhiều bộ xử lý. " +
                    "Phiên bản 'Tra cứu' của dịch vụ cũng có thể được sử dụng để chọn thông tin xác thực động trong quá trình chạy, " +
                    "dựa trên thuộc tính của FlowFile (nếu bộ xử lý có đầu vào FlowFile).")
            .identifiesControllerService(AzureStorageCredentialsService.class)
            .required(false)
            .build();

    public static final PropertyDescriptor MANAGED_IDENTITY_CLIENT_ID = new PropertyDescriptor.Builder()
            .name("managed-identity-client-id")
            .displayName("Client ID của Managed Identity")
            .description("Client ID của danh tính được quản lý (Managed Identity). Thuộc tính này bắt buộc khi sử dụng User Assigned Managed Identity để xác thực. " +
                    "Thuộc tính phải để trống nếu sử dụng System Assigned Managed Identity.")
            .sensitive(true)
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final PropertyDescriptor SERVICE_PRINCIPAL_TENANT_ID = new PropertyDescriptor.Builder()
            .name("service-principal-tenant-id")
            .displayName("Tenant ID của Service Principal")
            .description("Mã định danh Tenant của Azure Active Directory chứa Service Principal. Thuộc tính này bắt buộc khi sử dụng xác thực bằng Service Principal.")
            .sensitive(true)
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final PropertyDescriptor SERVICE_PRINCIPAL_CLIENT_ID = new PropertyDescriptor.Builder()
            .name("service-principal-client-id")
            .displayName("Client ID của Service Principal")
            .description("Client ID (hoặc Application ID) của ứng dụng có Service Principal. Thuộc tính này bắt buộc khi sử dụng xác thực bằng Service Principal.")
            .sensitive(true)
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final PropertyDescriptor SERVICE_PRINCIPAL_CLIENT_SECRET = new PropertyDescriptor.Builder()
            .name("service-principal-client-secret")
            .displayName("Mật khẩu của Service Principal")
            .description("Mật khẩu của ứng dụng/Client có Service Principal. Thuộc tính này bắt buộc khi sử dụng xác thực bằng Service Principal.")
            .sensitive(true)
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();


    private AzureStorageUtils() {
        // do not instantiate
    }

    /**
     * Create CloudBlobClient instance.
     * @param flowFile An incoming FlowFile can be used for NiFi Expression Language evaluation to derive
     *                 Account Name, Account Key or SAS Token. This can be null if not available.
     */
    public static CloudBlobClient createCloudBlobClient(ProcessContext context, ComponentLog logger, FlowFile flowFile) throws URISyntaxException {
        final AzureStorageCredentialsDetails storageCredentialsDetails = getStorageCredentialsDetails(context, flowFile);
        final CloudStorageAccount cloudStorageAccount = getCloudStorageAccount(storageCredentialsDetails);
        final CloudBlobClient cloudBlobClient = cloudStorageAccount.createCloudBlobClient();
        return cloudBlobClient;
    }

    public static CloudStorageAccount getCloudStorageAccount(final AzureStorageCredentialsDetails storageCredentialsDetails) throws URISyntaxException {
        final CloudStorageAccount cloudStorageAccount;
        if (storageCredentialsDetails instanceof AzureStorageEmulatorCredentialsDetails) {
            AzureStorageEmulatorCredentialsDetails emulatorCredentials = (AzureStorageEmulatorCredentialsDetails) storageCredentialsDetails;
            final String proxyUri = emulatorCredentials.getDevelopmentStorageProxyUri();
            if (proxyUri != null) {
                cloudStorageAccount = CloudStorageAccount.getDevelopmentStorageAccount(new URI(proxyUri));
            } else {
                cloudStorageAccount = CloudStorageAccount.getDevelopmentStorageAccount();
            }
        } else {
            cloudStorageAccount = new CloudStorageAccount(
                storageCredentialsDetails.getStorageCredentials(),
                true,
                storageCredentialsDetails.getStorageSuffix(),
                storageCredentialsDetails.getStorageAccountName());
        }
        return cloudStorageAccount;
    }

    public static AzureStorageCredentialsDetails getStorageCredentialsDetails(PropertyContext context, FlowFile flowFile) {
        final Map<String, String> attributes = flowFile != null ? flowFile.getAttributes() : Collections.emptyMap();

        final AzureStorageCredentialsService storageCredentialsService = context.getProperty(STORAGE_CREDENTIALS_SERVICE).asControllerService(AzureStorageCredentialsService.class);

        if (storageCredentialsService != null) {
            return storageCredentialsService.getStorageCredentialsDetails(attributes);
        } else {
            return createStorageCredentialsDetails(context, attributes);
        }
    }

    public static AzureStorageCredentialsDetails createStorageCredentialsDetails(PropertyContext context, Map<String, String> attributes) {
        final String accountName = context.getProperty(ACCOUNT_NAME).evaluateAttributeExpressions(attributes).getValue();
        final String storageSuffix = context.getProperty(ENDPOINT_SUFFIX).evaluateAttributeExpressions(attributes).getValue();
        final String accountKey = context.getProperty(ACCOUNT_KEY).evaluateAttributeExpressions(attributes).getValue();
        final String sasToken = context.getProperty(PROP_SAS_TOKEN).evaluateAttributeExpressions(attributes).getValue();

        if (StringUtils.isBlank(accountName)) {
            throw new IllegalArgumentException(String.format("'%s' must not be empty.", ACCOUNT_NAME.getDisplayName()));
        }

        StorageCredentials storageCredentials;

        if (StringUtils.isNotBlank(accountKey)) {
            storageCredentials = new StorageCredentialsAccountAndKey(accountName, accountKey);
        } else if (StringUtils.isNotBlank(sasToken)) {
            storageCredentials = new StorageCredentialsSharedAccessSignature(sasToken);
        } else {
            throw new IllegalArgumentException(String.format("Either '%s' or '%s' must be defined.", ACCOUNT_KEY.getDisplayName(), PROP_SAS_TOKEN.getDisplayName()));
        }

        return new AzureStorageCredentialsDetails(accountName, storageSuffix, storageCredentials);
    }

    public static Collection<ValidationResult> validateCredentialProperties(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        final String storageCredentials = validationContext.getProperty(STORAGE_CREDENTIALS_SERVICE).getValue();
        final String accountName = validationContext.getProperty(ACCOUNT_NAME).getValue();
        final String accountKey = validationContext.getProperty(ACCOUNT_KEY).getValue();
        final String sasToken = validationContext.getProperty(PROP_SAS_TOKEN).getValue();
        final String endpointSuffix = validationContext.getProperty(ENDPOINT_SUFFIX).getValue();

        if (!((StringUtils.isNotBlank(storageCredentials) && StringUtils.isBlank(accountName) && StringUtils.isBlank(accountKey) && StringUtils.isBlank(sasToken))
                || (StringUtils.isBlank(storageCredentials) && StringUtils.isNotBlank(accountName) && StringUtils.isNotBlank(accountKey) && StringUtils.isBlank(sasToken))
                || (StringUtils.isBlank(storageCredentials) && StringUtils.isNotBlank(accountName) && StringUtils.isBlank(accountKey) && StringUtils.isNotBlank(sasToken)))) {
            results.add(new ValidationResult.Builder().subject("AzureStorageUtils Credentials")
                    .valid(false)
                    .explanation("either " + STORAGE_CREDENTIALS_SERVICE.getDisplayName()
                            + ", or " + ACCOUNT_NAME.getDisplayName() + " with " + ACCOUNT_KEY.getDisplayName()
                            + " or " + ACCOUNT_NAME.getDisplayName() + " with " + PROP_SAS_TOKEN.getDisplayName() + " must be specified")
                    .build());
        }

        if(StringUtils.isNotBlank(storageCredentials) && StringUtils.isNotBlank(endpointSuffix)) {
            String errMsg = "Either " + STORAGE_CREDENTIALS_SERVICE.getDisplayName() + " or " + ENDPOINT_SUFFIX.getDisplayName()
                + " should be specified, not both.";
            results.add(new ValidationResult.Builder().subject("AzureStorageUtils Credentials")
                        .explanation(errMsg)
                        .build());
        }

        return results;
    }

    private static final ProxySpec[] PROXY_SPECS = {ProxySpec.HTTP, ProxySpec.SOCKS};
    public static final PropertyDescriptor PROXY_CONFIGURATION_SERVICE
            = ProxyConfiguration.createProxyConfigPropertyDescriptor(false, PROXY_SPECS);

    public static void validateProxySpec(ValidationContext context, Collection<ValidationResult> results) {
        ProxyConfiguration.validateProxySpec(context, results, PROXY_SPECS);
    }

    public static void setProxy(final OperationContext operationContext, final ProcessContext processContext) {
        final ProxyConfiguration proxyConfig = ProxyConfiguration.getConfiguration(processContext);
        operationContext.setProxy(proxyConfig.createProxy());
    }

    /**
     *
     * Creates the {@link ProxyOptions proxy options} that {@link HttpClient} will use.
     *
     * @param propertyContext to supply Proxy configurations
     * @return {@link ProxyOptions proxy options}, null if Proxy is not set
     */
    public static ProxyOptions getProxyOptions(final PropertyContext propertyContext) {
        final ProxyConfiguration proxyConfiguration = ProxyConfiguration.getConfiguration(propertyContext);

        if (proxyConfiguration != ProxyConfiguration.DIRECT_CONFIGURATION) {

            final ProxyOptions proxyOptions = new ProxyOptions(
                    getProxyType(proxyConfiguration),
                    new InetSocketAddress(proxyConfiguration.getProxyServerHost(), proxyConfiguration.getProxyServerPort()));

            final String proxyUserName = proxyConfiguration.getProxyUserName();
            final String proxyUserPassword = proxyConfiguration.getProxyUserPassword();
            if (proxyUserName != null && proxyUserPassword != null) {
                proxyOptions.setCredentials(proxyUserName, proxyUserPassword);
            }

            return proxyOptions;
        }

        return null;
    }

    private static ProxyOptions.Type getProxyType(ProxyConfiguration proxyConfiguration) {
        if (proxyConfiguration.getProxyType() == Proxy.Type.HTTP) {
            return ProxyOptions.Type.HTTP;
        } else if (proxyConfiguration.getProxyType() == Proxy.Type.SOCKS) {
            final SocksVersion socksVersion = proxyConfiguration.getSocksVersion();
            return ProxyOptions.Type.valueOf(socksVersion.name());
        } else {
            throw new IllegalArgumentException("Unsupported proxy type: " + proxyConfiguration.getProxyType());
        }
    }
}
