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
package org.apache.nifi.atlas.reporting;

import com.sun.jersey.api.client.ClientResponse;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.hook.AtlasHook;
import org.apache.atlas.security.SecurityProperties;
import org.apache.atlas.utils.AtlasPathExtractorUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.DeprecationNotice;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.atlas.NiFiAtlasClient;
import org.apache.nifi.atlas.NiFiFlow;
import org.apache.nifi.atlas.NiFiFlowAnalyzer;
import org.apache.nifi.atlas.hook.NiFiAtlasHook;
import org.apache.nifi.atlas.provenance.AnalysisContext;
import org.apache.nifi.atlas.provenance.FilesystemPathsLevel;
import org.apache.nifi.atlas.provenance.StandardAnalysisContext;
import org.apache.nifi.atlas.provenance.lineage.CompleteFlowPathLineage;
import org.apache.nifi.atlas.provenance.lineage.LineageStrategy;
import org.apache.nifi.atlas.provenance.lineage.SimpleFlowPathLineage;
import org.apache.nifi.atlas.resolver.NamespaceResolver;
import org.apache.nifi.atlas.resolver.NamespaceResolvers;
import org.apache.nifi.atlas.resolver.RegexNamespaceResolver;
import org.apache.nifi.atlas.security.AtlasAuthN;
import org.apache.nifi.atlas.security.Basic;
import org.apache.nifi.atlas.security.Kerberos;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.kerberos.KerberosCredentialsService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceRepository;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.EventAccess;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.util.provenance.ProvenanceEventConsumer;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.StringSelector;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.nifi.reporting.util.provenance.ProvenanceEventConsumer.PROVENANCE_BATCH_SIZE;
import static org.apache.nifi.reporting.util.provenance.ProvenanceEventConsumer.PROVENANCE_START_POSITION;

@DeprecationNotice(reason = "Dự kiến sẽ bị xóa trong Life 2.0")
@Tags({"atlas", "lineage"})
@CapabilityDescription("Báo cáo dòng dõi dữ liệu cấp độ tập dữ liệu của luồng Life cho Apache Atlas." +
        " Dòng dõi dữ liệu từ đầu đến cuối qua các môi trường Life và các hệ thống khác có thể được báo cáo nếu chúng" +
        " được kết nối bằng các giao thức và tập dữ liệu khác nhau, chẳng hạn như Life Site-to-Site, chủ đề Kafka hoặc bảng Hive ... v.v." +
        " Dòng dõi dữ liệu Atlas được báo cáo bởi tác vụ báo cáo này có thể hữu ích để nắm bắt các mối quan hệ cấp cao giữa các quy trình và tập dữ liệu," +
        " ngoài các sự kiện xuất xứ của Life cung cấp dòng dõi dữ liệu chi tiết ở cấp độ sự kiện." +
        " Xem 'Chi tiết bổ sung' để biết thêm mô tả và các hạn chế.")
@Stateful(scopes = Scope.LOCAL, description = "Lưu trữ Id sự kiện cuối cùng của Tác vụ Báo cáo để khi khởi động lại, tác vụ biết nó đã dừng ở đâu.")
@DynamicProperty(name = "hostnamePattern.<namespace>", value = "Các mẫu Regex của tên máy chủ",
                 description = RegexNamespaceResolver.PATTERN_PROPERTY_PREFIX_DESC, expressionLanguageScope = ExpressionLanguageScope.VARIABLE_REGISTRY)
// Để mỗi phiên bản tác vụ báo cáo có các đối tượng tĩnh riêng, chẳng hạn như KafkaNotification.
@RequiresInstanceClassLoading
public class ReportLineageToAtlas extends AbstractReportingTask {

    private static final String ATLAS_URL_DELIMITER = ",";
    static final PropertyDescriptor ATLAS_URLS = new PropertyDescriptor.Builder()
            .name("atlas-urls")
            .displayName("Các URL của Atlas")
            .description("URL của các Máy chủ Atlas được phân tách bằng dấu phẩy" +
                    " (ví dụ: http://atlas-server-hostname:21000 hoặc https://atlas-server-hostname:21443)." +
                    " Để truy cập Atlas phía sau cổng Knox, hãy chỉ định URL cổng Knox" +
                    " (ví dụ: https://knox-hostname:8443/gateway/{topology-name}/atlas)." +
                    " Nếu không được chỉ định, 'atlas.rest.address' trong Tệp cấu hình Atlas sẽ được sử dụng.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor ATLAS_CONNECT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("atlas-connect-timeout")
            .displayName("Thời gian chờ kết nối Atlas")
            .description("Thời gian chờ tối đa để kết nối đến Atlas.")
            .required(true)
            .defaultValue("60 giây")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor ATLAS_READ_TIMEOUT = new PropertyDescriptor.Builder()
            .name("atlas-read-timeout")
            .displayName("Thời gian chờ đọc Atlas")
            .description("Thời gian chờ tối đa để nhận phản hồi từ Atlas.")
            .required(true)
            .defaultValue("60 giây")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final AllowableValue ATLAS_AUTHN_BASIC = new AllowableValue("basic", "Cơ bản", "Sử dụng tên người dùng và mật khẩu.");
    static final AllowableValue ATLAS_AUTHN_KERBEROS = new AllowableValue("kerberos", "Kerberos", "Sử dụng tệp keytab Kerberos.");
    static final PropertyDescriptor ATLAS_AUTHN_METHOD = new PropertyDescriptor.Builder()
            .name("atlas-authentication-method")
            .displayName("Phương thức xác thực Atlas")
            .description("Chỉ định cách xác thực tác vụ báo cáo này với máy chủ Atlas.")
            .required(true)
            .allowableValues(ATLAS_AUTHN_BASIC, ATLAS_AUTHN_KERBEROS)
            .defaultValue(ATLAS_AUTHN_BASIC.getValue())
            .build();

    public static final PropertyDescriptor ATLAS_USER = new PropertyDescriptor.Builder()
            .name("atlas-username")
            .displayName("Tên người dùng Atlas")
            .description("Tên người dùng để giao tiếp với Atlas.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor ATLAS_PASSWORD = new PropertyDescriptor.Builder()
            .name("atlas-password")
            .displayName("Mật khẩu Atlas")
            .description("Mật khẩu để giao tiếp với Atlas.")
            .required(false)
            .sensitive(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor ATLAS_CONF_DIR = new PropertyDescriptor.Builder()
            .name("atlas-conf-dir")
            .displayName("Thư mục cấu hình Atlas")
            .description("Đường dẫn thư mục chứa tệp 'atlas-application.properties'." +
                    " Nếu không được chỉ định và 'Tạo tệp cấu hình Atlas' bị vô hiệu hóa," +
                    " thì, tệp 'atlas-application.properties' trong classpath gốc sẽ được sử dụng.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.DIRECTORY)
            // Atlas tạo ssl-client.xml trong thư mục này và sau đó tải nó từ classpath
            .dynamicallyModifiesClasspath(true)
            .build();

    public static final PropertyDescriptor ATLAS_NIFI_URL = new PropertyDescriptor.Builder()
            .name("atlas-nifi-url")
            .displayName("URL Life cho Atlas")
            .description("URL Life được sử dụng trong Atlas để đại diện cho cụm Life này (hoặc phiên bản độc lập)." +
                    " Khuyến nghị sử dụng một URL có thể truy cập từ xa thay vì sử dụng 'localhost'.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor ATLAS_DEFAULT_CLUSTER_NAME = new PropertyDescriptor.Builder()
            .name("atlas-default-cluster-name")
            .displayName("Không gian tên siêu dữ liệu mặc định của Atlas")
            .description("Không gian tên cho các thực thể Atlas được báo cáo bởi Tác vụ Báo cáo này." +
                    " Nếu không được chỉ định, 'atlas.metadata.namespace' hoặc 'atlas.cluster.name' (ưu tiên cái trước) trong Tệp cấu hình Atlas sẽ được sử dụng." +
                    " Nhiều ánh xạ có thể được cấu hình bởi các thuộc tính do người dùng định nghĩa." +
                    " Xem 'Chi tiết bổ sung...' để biết thêm.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor ATLAS_CONF_CREATE = new PropertyDescriptor.Builder()
            .name("atlas-conf-create")
            .displayName("Tạo tệp cấu hình Atlas")
            .description("Nếu được bật, tệp 'atlas-application.properties' sẽ được tạo trong 'Thư mục cấu hình Atlas'" +
                    " tự động khi Tác vụ Báo cáo này bắt đầu." +
                    " Lưu ý rằng tệp cấu hình hiện có sẽ bị ghi đè.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("ssl-context-service")
            .displayName("Dịch vụ ngữ cảnh SSL")
            .description("Chỉ định Dịch vụ ngữ cảnh SSL sẽ sử dụng để giao tiếp với Atlas và Kafka.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    static final PropertyDescriptor KAFKA_BOOTSTRAP_SERVERS = new PropertyDescriptor.Builder()
            .name("kafka-bootstrap-servers")
            .displayName("Các máy chủ Bootstrap của Kafka")
            .description("Các máy chủ Bootstrap của Kafka để gửi các thông báo hook của Atlas dựa trên các sự kiện xuất xứ của Life." +
                    " Ví dụ: 'localhost:9092'" +
                    " LƯU Ý: Sau khi tác vụ báo cáo này đã bắt đầu, cần phải khởi động lại Life để thay đổi thuộc tính này" +
                    " vì thư viện Atlas giữ một tham chiếu tĩnh không thể sửa đổi đến máy khách Kafka.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final AllowableValue SEC_PLAINTEXT = new AllowableValue("PLAINTEXT", "PLAINTEXT", "PLAINTEXT");
    static final AllowableValue SEC_SSL = new AllowableValue("SSL", "SSL", "SSL");
    static final AllowableValue SEC_SASL_PLAINTEXT = new AllowableValue("SASL_PLAINTEXT", "SASL_PLAINTEXT", "SASL_PLAINTEXT");
    static final AllowableValue SEC_SASL_SSL = new AllowableValue("SASL_SSL", "SASL_SSL", "SASL_SSL");
    static final PropertyDescriptor KAFKA_SECURITY_PROTOCOL = new PropertyDescriptor.Builder()
            .name("kafka-security-protocol")
            .displayName("Giao thức bảo mật Kafka")
            .description("Giao thức được sử dụng để giao tiếp với các broker Kafka để gửi thông báo hook của Atlas." +
                    " Tương ứng với thuộc tính 'security.protocol' của Kafka.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues(SEC_PLAINTEXT, SEC_SSL, SEC_SASL_PLAINTEXT, SEC_SASL_SSL)
            .defaultValue(SEC_PLAINTEXT.getValue())
            .build();

    public static final PropertyDescriptor KERBEROS_PRINCIPAL = new PropertyDescriptor.Builder()
            .name("nifi-kerberos-principal")
            .displayName("Principal Kerberos")
            .description("Principal Kerberos cho phiên bản Life này để truy cập API Atlas và các broker Kafka." +
                    " Nếu không được đặt, dự kiến sẽ đặt một tệp cấu hình JAAS trong các thuộc tính JVM được định nghĩa trong tệp bootstrap.conf." +
                    " Principal này sẽ được đặt vào thuộc tính 'sasl.jaas.config' của Kafka.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    public static final PropertyDescriptor KERBEROS_KEYTAB = new PropertyDescriptor.Builder()
            .name("nifi-kerberos-keytab")
            .displayName("Keytab Kerberos")
            .description("Keytab Kerberos cho phiên bản Life này để truy cập API Atlas và các broker Kafka." +
                    " Nếu không được đặt, dự kiến sẽ đặt một tệp cấu hình JAAS trong các thuộc tính JVM được định nghĩa trong tệp bootstrap.conf." +
                    " Principal này sẽ được đặt vào thuộc tính 'sasl.jaas.config' của Kafka.")
            .required(false)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    public static final PropertyDescriptor KERBEROS_CREDENTIALS_SERVICE = new PropertyDescriptor.Builder()
        .name("kerberos-credentials-service")
        .displayName("Dịch vụ chứng thực Kerberos")
        .description("Chỉ định Dịch vụ kiểm soát chứng thực Kerberos (Kerberos Credentials Controller Service) sẽ được sử dụng để xác thực với Kerberos")
        .identifiesControllerService(KerberosCredentialsService.class)
        .required(false)
        .build();


    static final PropertyDescriptor KAFKA_KERBEROS_SERVICE_NAME = new PropertyDescriptor.Builder()
            .name("kafka-kerberos-service-name")
            .displayName("Tên dịch vụ Kerberos của Kafka")
            .description("Tên dịch vụ khớp với tên chính của máy chủ Kafka được cấu hình trong tệp JAAS của broker." +
                    " Điều này có thể được định nghĩa trong cấu hình JAAS của Kafka hoặc trong cấu hình của Kafka." +
                    " Tương ứng với thuộc tính 'security.protocol' của Kafka." +
                    " Nó sẽ bị bỏ qua trừ khi một trong các tùy chọn SASL của <Giao thức bảo mật> được chọn.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("kafka")
            .build();

    static final AllowableValue LINEAGE_STRATEGY_SIMPLE_PATH = new AllowableValue("SimplePath", "Đường dẫn đơn giản",
            "Ánh xạ các sự kiện xuất xứ của Life và các DataSet Atlas mục tiêu tới các Quy trình Atlas 'nifi_flow_path' được tạo tĩnh." +
                    " Xem thêm 'Chi tiết bổ sung'.");
    static final AllowableValue LINEAGE_STRATEGY_COMPLETE_PATH = new AllowableValue("CompletePath", "Đường dẫn hoàn chỉnh",
            "Tạo các Quy trình Atlas 'nifi_flow_path' riêng biệt cho mỗi sự kết hợp DataSet đầu vào và đầu ra khác nhau" +
                    " bằng cách xem xét tuyến đường hoàn chỉnh cho một FlowFile nhất định. Xem thêm 'Chi tiết bổ sung'.");

    static final PropertyDescriptor LINEAGE_STRATEGY = new PropertyDescriptor.Builder()
            .name("nifi-lineage-strategy")
            .displayName("Chiến lược dòng dõi dữ liệu")
            .description("Chỉ định mức độ chi tiết về cách luồng dữ liệu Life sẽ được báo cáo cho Atlas." +
                    " LƯU Ý: Rất khuyến khích tiếp tục sử dụng cùng một chiến lược sau khi tác vụ báo cáo này bắt đầu để giữ cho dữ liệu Atlas sạch sẽ." +
                    " Việc chuyển đổi chiến lược sẽ không xóa các thực thể Atlas được tạo bởi chiến lược cũ." +
                    " Việc có các thực thể hỗn hợp được tạo bởi các chiến lược khác nhau làm cho biểu đồ dòng dõi dữ liệu của Atlas trở nên nhiễu." +
                    " Để biết mô tả chi tiết hơn về từng chiến lược và sự khác biệt, hãy tham khảo phần 'Chiến lược dòng dõi dữ liệu Life' trong Chi tiết bổ sung.")
            .required(true)
            .allowableValues(LINEAGE_STRATEGY_SIMPLE_PATH, LINEAGE_STRATEGY_COMPLETE_PATH)
            .defaultValue(LINEAGE_STRATEGY_SIMPLE_PATH.getValue())
            .build();

    static final AllowableValue AWS_S3_MODEL_VERSION_V1 = new AllowableValue("v1", "v1",
            "Tạo các thực thể thư mục AWS S3 phiên bản 1 (aws_s3_pseudo_dir).");
    static final AllowableValue AWS_S3_MODEL_VERSION_V2 = new AllowableValue(AtlasPathExtractorUtil.AWS_S3_ATLAS_MODEL_VERSION_V2, "v2",
            "Tạo các thực thể thư mục AWS S3 phiên bản 2 (aws_s3_v2_directory).");

    static final PropertyDescriptor AWS_S3_MODEL_VERSION = new PropertyDescriptor.Builder()
            .name("aws-s3-model-version")
            .displayName("Phiên bản mô hình AWS S3")
            .description("Chỉ định loại thực thể thư mục AWS S3 nào sẽ được tạo trong Atlas cho các URI chuyển tiếp s3a:// (ví dụ: PutHDFS với tích hợp S3)." +
                    " LƯU Ý: Rất khuyến khích tiếp tục sử dụng cùng một phiên bản mô hình thực thể AWS S3 sau khi tác vụ báo cáo này bắt đầu để giữ cho dữ liệu Atlas sạch sẽ." +
                    " Việc chuyển đổi phiên bản sẽ không xóa các thực thể Atlas hiện có được tạo bởi phiên bản cũ, cũng không di chuyển chúng sang phiên bản mới.")
            .required(true)
            .allowableValues(AWS_S3_MODEL_VERSION_V1, AWS_S3_MODEL_VERSION_V2)
            .defaultValue(AWS_S3_MODEL_VERSION_V2.getValue())
            .build();

    static final AllowableValue FILESYSTEM_PATHS_LEVEL_FILE = new AllowableValue(FilesystemPathsLevel.FILE.name(), FilesystemPathsLevel.FILE.getDisplayName(),
            "Tạo đường dẫn ở cấp độ Tệp.");
    static final AllowableValue FILESYSTEM_PATHS_LEVEL_DIRECTORY = new AllowableValue(FilesystemPathsLevel.DIRECTORY.name(), FilesystemPathsLevel.DIRECTORY.getDisplayName(),
            "Tạo đường dẫn ở cấp độ Thư mục.");

    static final PropertyDescriptor FILESYSTEM_PATHS_LEVEL = new PropertyDescriptor.Builder()
            .name("filesystem-paths-level")
            .displayName("Cấp độ thực thể đường dẫn hệ thống tệp")
            .description("Chỉ định cách các thực thể đường dẫn hệ thống tệp (fs_path và hdfs_path) sẽ được ghi lại trong Atlas: Cấp độ Tệp hoặc Thư mục. Trong trường hợp cấp độ Tệp, mỗi thực thể tệp riêng lẻ " +
                    "sẽ được gửi đến Atlas như một thực thể riêng biệt với đường dẫn đầy đủ bao gồm cả tên tệp. Cấp độ thư mục chỉ ghi lại đường dẫn của thư mục cha mà không có tên tệp. " +
                    "Cài đặt này ảnh hưởng đến các bộ xử lý làm việc với các tệp, như GetFile hoặc PutHDFS. LƯU Ý: Mặc dù giá trị mặc định là cấp độ Tệp vì lý do tương thích ngược, " +
                    "rất khuyến khích đặt nó thành cấp độ Thư mục vì việc ghi nhật ký cấp độ Tệp có thể tạo ra một số lượng lớn các thực thể trong Atlas.")
            .required(true)
            .allowableValues(FILESYSTEM_PATHS_LEVEL_FILE, FILESYSTEM_PATHS_LEVEL_DIRECTORY)
            .defaultValue(FILESYSTEM_PATHS_LEVEL_FILE.getValue())
            .build();
    private static final String ATLAS_PROPERTIES_FILENAME = "atlas-application.properties";
    private static final String ATLAS_PROPERTY_CLIENT_CONNECT_TIMEOUT_MS = "atlas.client.connectTimeoutMSecs";
    private static final String ATLAS_PROPERTY_CLIENT_READ_TIMEOUT_MS = "atlas.client.readTimeoutMSecs";
    private static final String ATLAS_PROPERTY_METADATA_NAMESPACE = "atlas.metadata.namespace";
    private static final String ATLAS_PROPERTY_CLUSTER_NAME = "atlas.cluster.name";
    private static final String ATLAS_PROPERTY_REST_ADDRESS = "atlas.rest.address";
    private static final String ATLAS_PROPERTY_ENABLE_TLS = SecurityProperties.TLS_ENABLED;
    private static final String ATLAS_KAFKA_PREFIX = "atlas.kafka.";
    private static final String ATLAS_PROPERTY_KAFKA_BOOTSTRAP_SERVERS = ATLAS_KAFKA_PREFIX + "bootstrap.servers";
    private static final String ATLAS_PROPERTY_KAFKA_CLIENT_ID = ATLAS_KAFKA_PREFIX + ProducerConfig.CLIENT_ID_CONFIG;

    private static final String SSL_CLIENT_XML_FILENAME = SecurityProperties.SSL_CLIENT_PROPERTIES;
    private static final String SSL_CLIENT_XML_TRUSTSTORE_LOCATION = "ssl.client.truststore.location";
    private static final String SSL_CLIENT_XML_TRUSTSTORE_PASSWORD = "ssl.client.truststore.password";
    private static final String SSL_CLIENT_XML_TRUSTSTORE_TYPE = "ssl.client.truststore.type";

    private final ServiceLoader<NamespaceResolver> namespaceResolverLoader = ServiceLoader.load(NamespaceResolver.class);
    private volatile AtlasAuthN atlasAuthN;
    private volatile Properties atlasProperties;
    private volatile boolean isTypeDefCreated = false;
    private volatile String defaultMetadataNamespace;

    private volatile ProvenanceEventConsumer consumer;
    private volatile NamespaceResolvers namespaceResolvers;
    private volatile NiFiAtlasHook nifiAtlasHook;
    private volatile LineageStrategy lineageStrategy;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        // Basic atlas config
        properties.add(ATLAS_URLS);
        properties.add(ATLAS_CONF_DIR);
        properties.add(ATLAS_CONF_CREATE);
        properties.add(ATLAS_DEFAULT_CLUSTER_NAME);

        // General config used by the processor
        properties.add(LINEAGE_STRATEGY);
        properties.add(PROVENANCE_START_POSITION);
        properties.add(PROVENANCE_BATCH_SIZE);
        properties.add(ATLAS_NIFI_URL);
        properties.add(ATLAS_AUTHN_METHOD);
        properties.add(ATLAS_USER);
        properties.add(ATLAS_PASSWORD);

        // Following properties are required if ATLAS_CONF_CREATE is enabled.
        // Otherwise should be left blank.
        // Will be used by the atlas client by reading the values from the atlas config file
        properties.add(KERBEROS_CREDENTIALS_SERVICE);
        properties.add(KERBEROS_PRINCIPAL);
        properties.add(KERBEROS_KEYTAB);
        properties.add(SSL_CONTEXT_SERVICE);
        properties.add(KAFKA_BOOTSTRAP_SERVERS);
        properties.add(KAFKA_SECURITY_PROTOCOL);
        properties.add(KAFKA_KERBEROS_SERVICE_NAME);
        properties.add(ATLAS_CONNECT_TIMEOUT);
        properties.add(ATLAS_READ_TIMEOUT);

        // Provenance event analyzer specific properties
        properties.add(AWS_S3_MODEL_VERSION);
        properties.add(FILESYSTEM_PATHS_LEVEL);

        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        for (NamespaceResolver resolver : namespaceResolverLoader) {
            final PropertyDescriptor propertyDescriptor = resolver.getSupportedDynamicPropertyDescriptor(propertyDescriptorName);
            if(propertyDescriptor != null) {
                return propertyDescriptor;
            }
        }
        return null;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        final Collection<ValidationResult> results = new ArrayList<>();

        final Set<String> schemes = new HashSet<>();
        String atlasUrls = context.getProperty(ATLAS_URLS).evaluateAttributeExpressions().getValue();
        if (!StringUtils.isEmpty(atlasUrls)) {
            Arrays.stream(atlasUrls.split(ATLAS_URL_DELIMITER))
                .map(String::trim)
                .forEach(input -> {
                    try {
                        schemes.add(Objects.requireNonNull(URI.create(input).getScheme()));
                    } catch (Exception e) {
                        results.add(new ValidationResult.Builder().subject(ATLAS_URLS.getDisplayName()).input(input)
                                .explanation("contains invalid URI: " + e).valid(false).build());
                    }
                });
        }

        if (schemes.size() > 1) {
            results.add(new ValidationResult.Builder().subject(ATLAS_URLS.getDisplayName())
                    .explanation("URLs with multiple schemes have been specified").valid(false).build());
        }

        final String atlasAuthNMethod = context.getProperty(ATLAS_AUTHN_METHOD).getValue();
        final AtlasAuthN atlasAuthN = getAtlasAuthN(atlasAuthNMethod);
        results.addAll(atlasAuthN.validate(context));

        synchronized (namespaceResolverLoader) {
            // ServiceLoader is not thread-safe and customValidate() may be executed on multiple threads in parallel,
            // especially if the component has a property with dynamicallyModifiesClasspath(true)
            // and the component gets reloaded due to this when the property has been modified
            namespaceResolverLoader.forEach(resolver -> results.addAll(resolver.validate(context)));
        }

        if (context.getProperty(ATLAS_CONF_CREATE).asBoolean()) {

            Stream.of(ATLAS_URLS, ATLAS_CONF_DIR, ATLAS_DEFAULT_CLUSTER_NAME, KAFKA_BOOTSTRAP_SERVERS)
                    .filter(p -> !context.getProperty(p).isSet())
                    .forEach(p -> results.add(new ValidationResult.Builder()
                            .subject(p.getDisplayName())
                            .explanation("required to create Atlas configuration file.")
                            .valid(false).build()));

            validateKafkaProperties(context, results);
        }

        return results;
    }

    private void validateKafkaProperties(ValidationContext context, Collection<ValidationResult> results) {
        final String kafkaSecurityProtocol = context.getProperty(KAFKA_SECURITY_PROTOCOL).getValue();

        if ((SEC_SSL.equals(kafkaSecurityProtocol) || SEC_SASL_SSL.equals(kafkaSecurityProtocol))
                && !context.getProperty(SSL_CONTEXT_SERVICE).isSet()) {
            results.add(new ValidationResult.Builder()
                    .subject(SSL_CONTEXT_SERVICE.getDisplayName())
                    .explanation("required by SSL Kafka connection")
                    .valid(false)
                    .build());
        }

        if (SEC_SASL_PLAINTEXT.equals(kafkaSecurityProtocol) || SEC_SASL_SSL.equals(kafkaSecurityProtocol)) {
            final KerberosCredentialsService credentialsService = context.getProperty(ReportLineageToAtlas.KERBEROS_CREDENTIALS_SERVICE).asControllerService(KerberosCredentialsService.class);

            if (credentialsService == null || context.getControllerServiceLookup().isControllerServiceEnabled(credentialsService)) {
                String principal;
                String keytab;
                if (credentialsService == null) {
                    principal = context.getProperty(KERBEROS_PRINCIPAL).evaluateAttributeExpressions().getValue();
                    keytab = context.getProperty(KERBEROS_KEYTAB).evaluateAttributeExpressions().getValue();
                } else {
                    principal = credentialsService.getPrincipal();
                    keytab = credentialsService.getKeytab();
                }

                if (keytab == null || principal == null) {
                    results.add(new ValidationResult.Builder()
                            .subject("Kerberos Authentication")
                            .explanation("Keytab and Principal are required for Kerberos authentication with Apache Kafka.")
                            .valid(false)
                            .build());
                }
            }

            if (!context.getProperty(KAFKA_KERBEROS_SERVICE_NAME).isSet()) {
                results.add(new ValidationResult.Builder()
                        .subject(KAFKA_KERBEROS_SERVICE_NAME.getDisplayName())
                        .explanation("Required by Kafka SASL authentication.")
                        .valid(false)
                        .build());
            }
        }
    }

    @OnScheduled
    public void setup(ConfigurationContext context) throws Exception {
        // initAtlasClient has to be done first as it loads AtlasProperty.
        initAtlasProperties(context);
        initLineageStrategy(context);
        initNamespaceResolvers(context);
    }

    private void initLineageStrategy(ConfigurationContext context) throws IOException {
        nifiAtlasHook = new NiFiAtlasHook();

        final String strategy = context.getProperty(LINEAGE_STRATEGY).getValue();
        if (LINEAGE_STRATEGY_SIMPLE_PATH.equals(strategy)) {
            lineageStrategy = new SimpleFlowPathLineage();
        } else if (LINEAGE_STRATEGY_COMPLETE_PATH.equals(strategy)) {
            lineageStrategy = new CompleteFlowPathLineage();
        }

        lineageStrategy.setLineageContext(nifiAtlasHook);
        initProvenanceConsumer(context);
    }

    private void initNamespaceResolvers(ConfigurationContext context) {
        final Set<NamespaceResolver> loadedNamespaceResolvers = new LinkedHashSet<>();
        namespaceResolverLoader.forEach(resolver -> {
            resolver.configure(context);
            loadedNamespaceResolvers.add(resolver);
        });
        namespaceResolvers = new NamespaceResolvers(Collections.unmodifiableSet(loadedNamespaceResolvers), defaultMetadataNamespace);
    }


    private void initAtlasProperties(ConfigurationContext context) throws Exception {
        final String atlasAuthNMethod = context.getProperty(ATLAS_AUTHN_METHOD).getValue();

        final String confDirStr = context.getProperty(ATLAS_CONF_DIR).evaluateAttributeExpressions().getValue();
        final File confDir = confDirStr != null && !confDirStr.isEmpty() ? new File(confDirStr) : null;

        atlasProperties = new Properties();
        final File atlasPropertiesFile = new File(confDir, ATLAS_PROPERTIES_FILENAME);

        final Boolean createAtlasConf = context.getProperty(ATLAS_CONF_CREATE).asBoolean();
        if (!createAtlasConf) {
            // Load existing properties file.
            if (atlasPropertiesFile.isFile()) {
                getLogger().info("Loading {}", atlasPropertiesFile);
                try (InputStream in = new FileInputStream(atlasPropertiesFile)) {
                    atlasProperties.load(in);
                }
            } else {
                final String fileInClasspath = "/" + ATLAS_PROPERTIES_FILENAME;
                try (InputStream in = ReportLineageToAtlas.class.getResourceAsStream(fileInClasspath)) {
                    getLogger().info("Loading {} from classpath", fileInClasspath);
                    if (in == null) {
                        throw new ProcessException(String.format("Could not find %s in classpath." +
                                " Please add it to classpath," +
                                " or specify %s a directory containing Atlas properties file," +
                                " or enable %s to generate it.",
                                fileInClasspath, ATLAS_CONF_DIR.getDisplayName(), ATLAS_CONF_CREATE.getDisplayName()));
                    }
                    atlasProperties.load(in);
                }
            }
        }

        List<String> urls = parseAtlasUrls(context.getProperty(ATLAS_URLS));

        setValue(
            value -> defaultMetadataNamespace = value,
            () -> {
                throw new ProcessException("Default metadata namespace (or cluster name) is not defined.");
            },
            context.getProperty(ATLAS_DEFAULT_CLUSTER_NAME),
            atlasProperties.getProperty(ATLAS_PROPERTY_METADATA_NAMESPACE),
            atlasProperties.getProperty(ATLAS_PROPERTY_CLUSTER_NAME)
        );

        String atlasConnectTimeoutMs = context.getProperty(ATLAS_CONNECT_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue() + "";
        String atlasReadTimeoutMs = context.getProperty(ATLAS_READ_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue() + "";

        atlasAuthN = getAtlasAuthN(atlasAuthNMethod);
        atlasAuthN.configure(context);

        // Create Atlas configuration file if necessary.
        if (createAtlasConf) {
            // enforce synchronous notification sending (needed for the checkpointing in ProvenanceEventConsumer)
            atlasProperties.setProperty(AtlasHook.ATLAS_NOTIFICATION_ASYNCHRONOUS, "false");

            atlasProperties.put(ATLAS_PROPERTY_REST_ADDRESS, urls.stream().collect(Collectors.joining(ATLAS_URL_DELIMITER)));
            atlasProperties.put(ATLAS_PROPERTY_CLIENT_CONNECT_TIMEOUT_MS, atlasConnectTimeoutMs);
            atlasProperties.put(ATLAS_PROPERTY_CLIENT_READ_TIMEOUT_MS, atlasReadTimeoutMs);
            atlasProperties.put(ATLAS_PROPERTY_METADATA_NAMESPACE, defaultMetadataNamespace);
            atlasProperties.put(ATLAS_PROPERTY_CLUSTER_NAME, defaultMetadataNamespace);

            setAtlasSSLConfig(atlasProperties, context, urls, confDir);

            setKafkaConfig(atlasProperties, context);

            atlasAuthN.populateProperties(atlasProperties);

            try (FileOutputStream fos = new FileOutputStream(atlasPropertiesFile)) {
                String ts = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX")
                        .withZone(ZoneOffset.UTC)
                        .format(Instant.now());
                atlasProperties.store(fos, "Generated by Apache Life ReportLineageToAtlas ReportingTask at " + ts);
            }
        } else {
            // check if synchronous notification sending has been set (needed for the checkpointing in ProvenanceEventConsumer)
            String isAsync = atlasProperties.getProperty(AtlasHook.ATLAS_NOTIFICATION_ASYNCHRONOUS);
            if (isAsync == null || !isAsync.equalsIgnoreCase("false")) {
                throw new ProcessException("Atlas property '" + AtlasHook.ATLAS_NOTIFICATION_ASYNCHRONOUS + "' must be set to 'false' in " + ATLAS_PROPERTIES_FILENAME + "." +
                        " Sending notifications asynchronously is not supported by the reporting task.");
            }
        }

        getLogger().debug("Force reloading Atlas application properties.");
        ApplicationProperties.forceReload();

        if (confDir != null) {
            // If atlasConfDir is not set, atlas-application.properties will be searched under classpath.
            Properties props = System.getProperties();
            final String atlasConfProp = ApplicationProperties.ATLAS_CONFIGURATION_DIRECTORY_PROPERTY;
            props.setProperty(atlasConfProp, confDir.getAbsolutePath());
            getLogger().debug("{} has been set to: {}", atlasConfProp, props.getProperty(atlasConfProp));
        }
    }

    private List<String> parseAtlasUrls(final PropertyValue atlasUrlsProp) {
        List<String> atlasUrls = new ArrayList<>();

        setValue(
            value -> Arrays.stream(value.split(ATLAS_URL_DELIMITER))
                .map(String::trim)
                .forEach(urlString -> {
                        try {
                            URI.create(urlString).toURL();
                        } catch (Exception e) {
                            throw new ProcessException(e);
                        }
                        atlasUrls.add(urlString);
                    }
                ),
            () -> {
                throw new ProcessException("No Atlas URL has been specified! Set either the '" + ATLAS_URLS.getDisplayName() + "' " +
                    "property on the processor or the 'atlas.rest.address' property in the atlas configuration file.");
            },
            atlasUrlsProp,
            atlasProperties.getProperty(ATLAS_PROPERTY_REST_ADDRESS)
        );

        return atlasUrls;
    }

    private void setValue(Consumer<String> setter, Runnable emptyHandler, PropertyValue elEnabledPropertyValue, String... properties) {
        StringSelector valueSelector = StringSelector
            .of(elEnabledPropertyValue.evaluateAttributeExpressions().getValue())
            .or(properties);

        if (valueSelector.found()) {
            setter.accept(valueSelector.toString());
        } else {
            emptyHandler.run();
        }
    }

    private void setAtlasSSLConfig(Properties atlasProperties, ConfigurationContext context, List<String> urls, File confDir) throws Exception {
        boolean isAtlasApiSecure = urls.stream().anyMatch(url -> url.toLowerCase().startsWith("https"));
        atlasProperties.put(ATLAS_PROPERTY_ENABLE_TLS, String.valueOf(isAtlasApiSecure));

        deleteSslClientXml(confDir);

        if (isAtlasApiSecure) {
            SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
            if (sslContextService == null) {
                getLogger().warn("No SSLContextService configured, the system default truststore will be used.");
            } else if (!sslContextService.isTrustStoreConfigured()) {
                getLogger().warn("No truststore configured on SSLContextService, the system default truststore will be used.");
            } else {
                // create ssl-client.xml config file for Hadoop Security used by Atlas REST client,
                // Atlas would generate this file with hardcoded JKS keystore type,
                // in order to support other keystore types, we generate it ourselves
                createSslClientXml(confDir, sslContextService);
            }
        }
    }

    private void deleteSslClientXml(File confDir) throws Exception {
        Path sslClientXmlPath = new File(confDir, SSL_CLIENT_XML_FILENAME).toPath();
        try {
            Files.deleteIfExists(sslClientXmlPath);
        } catch (Exception e) {
            getLogger().error("Unable to delete SSL Client Configuration File {}", sslClientXmlPath, e);
            throw e;
        }
    }

    private void createSslClientXml(File confDir, SSLContextService sslContextService) throws Exception {
        File sslClientXmlFile = new File(confDir, SSL_CLIENT_XML_FILENAME);

        Configuration configuration = new Configuration(false);

        configuration.set(SSL_CLIENT_XML_TRUSTSTORE_LOCATION, sslContextService.getTrustStoreFile());
        configuration.set(SSL_CLIENT_XML_TRUSTSTORE_PASSWORD, sslContextService.getTrustStorePassword());
        configuration.set(SSL_CLIENT_XML_TRUSTSTORE_TYPE, sslContextService.getTrustStoreType());

        try (FileWriter fileWriter = new FileWriter(sslClientXmlFile)) {
            configuration.writeXml(fileWriter);
        } catch (Exception e) {
            getLogger().error("Unable to create SSL Client Configuration File {}", sslClientXmlFile, e);
            throw e;
        }
    }

    /**
     * In order to avoid authentication expiration issues (i.e. Kerberos ticket and DelegationToken expiration),
     * create Atlas client instance at every onTrigger execution.
     */
    protected NiFiAtlasClient createNiFiAtlasClient(ReportingContext context) {
        List<String> urls = parseAtlasUrls(context.getProperty(ATLAS_URLS));
        try {
            return new NiFiAtlasClient(atlasAuthN.createClient(urls.toArray(new String[]{})));
        } catch (final NullPointerException e) {
            throw new ProcessException(String.format("Failed to initialize Atlas client due to %s." +
                    " Make sure 'atlas-application.properties' is in the directory specified with %s" +
                    " or under root classpath if not specified.", e, ATLAS_CONF_DIR.getDisplayName()), e);
        }
    }

    private AtlasAuthN getAtlasAuthN(String atlasAuthNMethod) {
        final AtlasAuthN atlasAuthN;
        switch (atlasAuthNMethod) {
            case "basic" :
                atlasAuthN = new Basic();
                break;
            case "kerberos" :
                atlasAuthN = new Kerberos();
                break;
            default:
                throw new IllegalArgumentException(atlasAuthNMethod + " is not supported as an Atlas authentication method.");
        }
        return atlasAuthN;
    }

    private void initProvenanceConsumer(final ConfigurationContext context) throws IOException {
        consumer = new ProvenanceEventConsumer();
        consumer.setStartPositionValue(context.getProperty(PROVENANCE_START_POSITION).getValue());
        consumer.setBatchSize(context.getProperty(PROVENANCE_BATCH_SIZE).asInteger());
        consumer.addTargetEventType(lineageStrategy.getTargetEventTypes());
        consumer.setLogger(getLogger());
        consumer.setScheduled(true);
    }

    @OnUnscheduled
    public void onUnscheduled() {
        if (consumer != null) {
            // Tell provenance consumer to stop pulling more provenance events.
            // This should be called from @OnUnscheduled to stop the loop in the thread called from onTrigger.
            consumer.setScheduled(false);
        }
    }

    @OnStopped
    public void onStopped() {
        if (nifiAtlasHook != null) {
            nifiAtlasHook.close();
            nifiAtlasHook = null;
        }
    }

    @Override
    public void onTrigger(ReportingContext context) {

        final String clusterNodeId = context.getClusterNodeIdentifier();
        final boolean isClustered = context.isClustered();
        if (isClustered && isEmpty(clusterNodeId)) {
            // Clustered, but this node's ID is unknown. Not ready for processing yet.
            return;
        }

        // If standalone or being primary node in a Life cluster, this node is responsible for doing primary tasks.
        final boolean isResponsibleForPrimaryTasks = !isClustered || getNodeTypeProvider().isPrimary();

        try (final NiFiAtlasClient atlasClient = createNiFiAtlasClient(context)) {

            // Create Entity defs in Atlas if there's none yet.
            if (!isTypeDefCreated) {
                try {
                    if (isResponsibleForPrimaryTasks) {
                        // Create Life type definitions in Atlas type system.
                        atlasClient.registerNiFiTypeDefs(false);
                    } else {
                        // Otherwise, just check existence of Life type definitions.
                        if (!atlasClient.isNiFiTypeDefsRegistered()) {
                            getLogger().debug("Life type definitions are not ready in Atlas type system yet.");
                            return;
                        }
                    }
                    isTypeDefCreated = true;
                } catch (AtlasServiceException e) {
                    throw new RuntimeException("Failed to check and create Life flow type definitions in Atlas due to " + e, e);
                }
            }

            // Regardless of whether being a primary task node, each node has to analyse NiFiFlow.
            // Assuming each node has the same flow definition, that is guaranteed by Life cluster management mechanism.
            final NiFiFlow nifiFlow = createNiFiFlow(context, atlasClient);


            if (isResponsibleForPrimaryTasks) {
                try {
                    atlasClient.registerNiFiFlow(nifiFlow);
                } catch (AtlasServiceException e) {
                    throw new RuntimeException("Failed to register NiFI flow. " + e, e);
                }
            }

            // NOTE: There is a race condition between the primary node and other nodes.
            // If a node notifies an event related to a Life component which is not yet created by Life primary node,
            // then the notification message will fail due to having a reference to a non-existing entity.
            nifiAtlasHook.setAtlasClient(atlasClient);
            consumeNiFiProvenanceEvents(context, nifiFlow);
        }
    }

    private NiFiFlow createNiFiFlow(ReportingContext context, NiFiAtlasClient atlasClient) {
        final ProcessGroupStatus rootProcessGroup = context.getEventAccess().getGroupStatus("root");
        final String flowName = rootProcessGroup.getName();
        final String nifiUrl = context.getProperty(ATLAS_NIFI_URL).evaluateAttributeExpressions().getValue();
        final String nifiHostName = URI.create(nifiUrl).getHost();
        final String namespace = namespaceResolvers.fromHostNames(nifiHostName);

        NiFiFlow existingNiFiFlow = null;
        try {
            // Retrieve Existing NiFiFlow from Atlas.
            existingNiFiFlow = atlasClient.fetchNiFiFlow(rootProcessGroup.getId(), namespace);
        } catch (AtlasServiceException e) {
            if (ClientResponse.Status.NOT_FOUND.equals(e.getStatus())){
                getLogger().debug("Existing flow was not found for {}@{}", rootProcessGroup.getId(), namespace);
            } else {
                throw new RuntimeException("Failed to fetch existing NiFI flow. " + e, e);
            }
        }

        final NiFiFlow nifiFlow = existingNiFiFlow != null ? existingNiFiFlow : new NiFiFlow(rootProcessGroup.getId());
        nifiFlow.setFlowName(flowName);
        nifiFlow.setUrl(nifiUrl);
        nifiFlow.setNamespace(namespace);

        final NiFiFlowAnalyzer flowAnalyzer = new NiFiFlowAnalyzer();

        flowAnalyzer.analyzeProcessGroup(nifiFlow, rootProcessGroup);
        flowAnalyzer.analyzePaths(nifiFlow);

        return nifiFlow;
    }

    private void consumeNiFiProvenanceEvents(ReportingContext context, NiFiFlow nifiFlow) {
        final EventAccess eventAccess = context.getEventAccess();
        final String awsS3ModelVersion = context.getProperty(AWS_S3_MODEL_VERSION).getValue();
        final FilesystemPathsLevel filesystemPathsLevel = FilesystemPathsLevel.valueOf(context.getProperty(FILESYSTEM_PATHS_LEVEL).getValue());
        final AnalysisContext analysisContext = new StandardAnalysisContext(nifiFlow, namespaceResolvers,
                // FIXME: This class cast shouldn't be necessary to query lineage. Possible refactor target in next major update.
                (ProvenanceRepository)eventAccess.getProvenanceRepository(), awsS3ModelVersion, filesystemPathsLevel);
        consumer.consumeEvents(context, (componentMapHolder, events) -> {
            for (ProvenanceEventRecord event : events) {
                try {
                    lineageStrategy.processEvent(analysisContext, nifiFlow, event);
                } catch (Exception e) {
                    // If something went wrong, log it and continue with other records.
                    getLogger().error("Skipping failed analyzing event {}", event, e);
                }
            }
            nifiAtlasHook.commitMessages();
        });
    }

    private void setKafkaConfig(Map<Object, Object> mapToPopulate, PropertyContext context) {

        final String kafkaBootStrapServers = context.getProperty(KAFKA_BOOTSTRAP_SERVERS).evaluateAttributeExpressions().getValue();
        mapToPopulate.put(ATLAS_PROPERTY_KAFKA_BOOTSTRAP_SERVERS, kafkaBootStrapServers);
        mapToPopulate.put(ATLAS_PROPERTY_KAFKA_CLIENT_ID, String.format("%s.%s", getName(), getIdentifier()));

        final String kafkaSecurityProtocol = context.getProperty(KAFKA_SECURITY_PROTOCOL).getValue();
        mapToPopulate.put(ATLAS_KAFKA_PREFIX + "security.protocol", kafkaSecurityProtocol);

        // Translate SSLContext Service configuration into Kafka properties
        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        if (sslContextService != null && sslContextService.isKeyStoreConfigured()) {
            mapToPopulate.put(ATLAS_KAFKA_PREFIX + SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, sslContextService.getKeyStoreFile());
            mapToPopulate.put(ATLAS_KAFKA_PREFIX + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, sslContextService.getKeyStorePassword());
            final String keyPass = sslContextService.getKeyPassword() == null ? sslContextService.getKeyStorePassword() : sslContextService.getKeyPassword();
            mapToPopulate.put(ATLAS_KAFKA_PREFIX + SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPass);
            mapToPopulate.put(ATLAS_KAFKA_PREFIX + SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, sslContextService.getKeyStoreType());
        }

        if (sslContextService != null && sslContextService.isTrustStoreConfigured()) {
            mapToPopulate.put(ATLAS_KAFKA_PREFIX + SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslContextService.getTrustStoreFile());
            mapToPopulate.put(ATLAS_KAFKA_PREFIX + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, sslContextService.getTrustStorePassword());
            mapToPopulate.put(ATLAS_KAFKA_PREFIX + SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, sslContextService.getTrustStoreType());
        }

        if (SEC_SASL_PLAINTEXT.equals(kafkaSecurityProtocol) || SEC_SASL_SSL.equals(kafkaSecurityProtocol)) {
            setKafkaJaasConfig(mapToPopulate, context);
        }

    }

    /**
     * Populate Kafka JAAS properties for Atlas notification.
     * Since Atlas 0.8.1 uses Kafka client 0.10.0.0, we can not use 'sasl.jaas.config' property
     * as it is available since 0.10.2, implemented by KAFKA-4259.
     * Instead, this method uses old property names.
     * @param mapToPopulate Map of configuration properties
     * @param context Context
     */
    private void setKafkaJaasConfig(Map<Object, Object> mapToPopulate, PropertyContext context) {
        String keytab;
        String principal;
        final String explicitPrincipal = context.getProperty(KERBEROS_PRINCIPAL).evaluateAttributeExpressions().getValue();
        final String explicitKeytab = context.getProperty(KERBEROS_KEYTAB).evaluateAttributeExpressions().getValue();

        final KerberosCredentialsService credentialsService = context.getProperty(ReportLineageToAtlas.KERBEROS_CREDENTIALS_SERVICE).asControllerService(KerberosCredentialsService.class);

        if (credentialsService == null) {
            principal = explicitPrincipal;
            keytab = explicitKeytab;
        } else {
            principal = credentialsService.getPrincipal();
            keytab = credentialsService.getKeytab();
        }

        String serviceName = context.getProperty(KAFKA_KERBEROS_SERVICE_NAME).evaluateAttributeExpressions().getValue();
        if(StringUtils.isNotBlank(keytab) && StringUtils.isNotBlank(principal) && StringUtils.isNotBlank(serviceName)) {
            mapToPopulate.put("atlas.jaas.KafkaClient.loginModuleControlFlag", "required");
            mapToPopulate.put("atlas.jaas.KafkaClient.loginModuleName", "com.sun.security.auth.module.Krb5LoginModule");
            mapToPopulate.put("atlas.jaas.KafkaClient.option.keyTab", keytab);
            mapToPopulate.put("atlas.jaas.KafkaClient.option.principal", principal);
            mapToPopulate.put("atlas.jaas.KafkaClient.option.serviceName", serviceName);
            mapToPopulate.put("atlas.jaas.KafkaClient.option.storeKey", "True");
            mapToPopulate.put("atlas.jaas.KafkaClient.option.useKeyTab", "True");
            mapToPopulate.put("atlas.jaas.ticketBased-KafkaClient.loginModuleControlFlag", "required");
            mapToPopulate.put("atlas.jaas.ticketBased-KafkaClient.loginModuleName", "com.sun.security.auth.module.Krb5LoginModule");
            mapToPopulate.put("atlas.jaas.ticketBased-KafkaClient.option.useTicketCache", "true");
            mapToPopulate.put(ATLAS_KAFKA_PREFIX + "sasl.kerberos.service.name", serviceName);
        }
    }

}
