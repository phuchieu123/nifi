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
package org.apache.nifi.processors.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.NiFiOrcUtils;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.DeprecationNotice;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.hadoop.AbstractPutHDFSRecord;
import org.apache.nifi.processors.hadoop.record.HDFSRecordWriter;
import org.apache.nifi.processors.orc.record.ORCHDFSRecordWriter;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
@DeprecationNotice(reason = "Hỗ trợ Apache Hive 3 đã bị đánh dấu ngừng sử dụng và sẽ bị loại bỏ trong Apache NiFi 2.0")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"put", "ORC", "hadoop", "HDFS", "filesystem", "restricted", "record"})
@CapabilityDescription("Đọc các bản ghi từ FlowFile đến sử dụng Record Reader được cung cấp, và ghi các bản ghi đó "
        + "vào một tệp ORC tại vị trí/filesystem được chỉ định trong cấu hình.")
@ReadsAttribute(attribute = "filename", description = "Tên tệp để ghi lấy từ giá trị của thuộc tính này.")
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "Tên tệp được lưu trong thuộc tính này."),
        @WritesAttribute(attribute = "absolute.hdfs.path", description = "Đường dẫn tuyệt đối tới tệp được lưu trong thuộc tính này."),
        @WritesAttribute(attribute = "hadoop.file.url", description = "URL Hadoop của tệp được lưu trong thuộc tính này."),
        @WritesAttribute(attribute = "record.count", description = "Số lượng bản ghi đã ghi vào tệp ORC"),
        @WritesAttribute(attribute = "hive.ddl", description = "Tạo một phần khai báo Hive DDL để tạo bảng ngoài trong Hive từ thư mục đích. "
                + "Có thể sử dụng trong ReplaceText để thiết lập nội dung thành DDL này. Để hợp lệ, thêm \"LOCATION '<path_to_orc_file_in_hdfs>'\", "
                + "với đường dẫn là thư mục chứa tệp ORC này trên HDFS. Ví dụ, processor này có thể gửi flow files xuống ReplaceText để thiết lập nội dung "
                + "thành DDL này (cộng với mệnh đề LOCATION như mô tả), sau đó tới PutHiveQL để tạo bảng nếu nó chưa tồn tại.")
})
@Restricted(restrictions = {
        @Restriction(
                requiredPermission = RequiredPermission.WRITE_DISTRIBUTED_FILESYSTEM,
                explanation = "Cho phép người vận hành ghi vào bất kỳ tệp nào mà NiFi có quyền truy cập trên HDFS hoặc filesystem cục bộ.")
})
public class PutORC extends AbstractPutHDFSRecord {

    public static final String HIVE_DDL_ATTRIBUTE = "hive.ddl";

    public static final PropertyDescriptor ORC_CONFIGURATION_RESOURCES = new PropertyDescriptor.Builder()
            .name("putorc-config-resources")
            .displayName("Tệp cấu hình ORC")
            .description("Một tệp hoặc danh sách các tệp (ngăn cách bằng dấu phẩy) chứa cấu hình ORC (ví dụ: hive-site.xml). "
                    + "Nếu không có, Hadoop sẽ tìm kiếm trong classpath tệp 'hive-site.xml' hoặc dùng cấu hình mặc định. "
                    + "Vui lòng xem tài liệu ORC để biết thêm chi tiết.")
            .required(false)
            .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE)
            .build();

    public static final PropertyDescriptor STRIPE_SIZE = new PropertyDescriptor.Builder()
            .name("putorc-stripe-size")
            .displayName("Kích thước Stripe")
            .description("Kích thước bộ đệm trong bộ nhớ (tính bằng byte) để ghi các stripe vào tệp ORC")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("64 MB")
            .build();

    public static final PropertyDescriptor BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("putorc-buffer-size")
            .displayName("Kích thước Bộ đệm")
            .description("Kích thước tối đa của bộ đệm trong bộ nhớ (tính bằng byte) được sử dụng để nén và lưu một stripe. "
                    + "Đây là gợi ý cho ORC writer, có thể chọn kích thước nhỏ hơn dựa trên stripe và số cột để tối ưu ghi stripe và sử dụng bộ nhớ.")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("10 KB")
            .build();

    static final PropertyDescriptor HIVE_TABLE_NAME = new PropertyDescriptor.Builder()
            .name("putorc-hive-table-name")
            .displayName("Tên bảng Hive")
            .description("Tên bảng tùy chọn để đưa vào thuộc tính hive.ddl. DDL tạo ra có thể được sử dụng bởi "
                    + "PutHive3QL processor (sau PutHDFS) để tạo bảng dựa trên tệp ORC đã chuyển đổi. "
                    + "Nếu không cung cấp thuộc tính này, tên đầy đủ (bao gồm namespace) của bản ghi Avro đến sẽ được chuẩn hóa và sử dụng làm tên bảng.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor HIVE_FIELD_NAMES = new PropertyDescriptor.Builder()
            .name("putorc-hive-field-names")
            .displayName("Chuẩn hóa Tên cột cho Hive")
            .description("Có chuẩn hóa tên cột cho Hive hay không (ví dụ: chuyển thành chữ thường). Nếu tệp ORC sẽ "
                    + "là một phần của bảng Hive, nên đặt thành true. Để giữ nguyên tên cột gốc, đặt thành false.")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();


    public static final List<AllowableValue> COMPRESSION_TYPES;

    static {
        final List<AllowableValue> compressionTypes = new ArrayList<>();
        compressionTypes.add(new AllowableValue("NONE", "NONE", "No compression"));
        compressionTypes.add(new AllowableValue("ZLIB", "ZLIB", "ZLIB compression"));
        compressionTypes.add(new AllowableValue("SNAPPY", "SNAPPY", "Snappy compression"));
        compressionTypes.add(new AllowableValue("LZO", "LZO", "LZO compression"));
        COMPRESSION_TYPES = Collections.unmodifiableList(compressionTypes);
    }

    @Override
    public List<AllowableValue> getCompressionTypes(final ProcessorInitializationContext context) {
        return COMPRESSION_TYPES;
    }

    @Override
    public String getDefaultCompressionType(final ProcessorInitializationContext context) {
        return "NONE";
    }

    @Override
    public List<PropertyDescriptor> getAdditionalProperties() {
        final List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(ORC_CONFIGURATION_RESOURCES);
        _propertyDescriptors.add(STRIPE_SIZE);
        _propertyDescriptors.add(BUFFER_SIZE);
        _propertyDescriptors.add(HIVE_TABLE_NAME);
        _propertyDescriptors.add(HIVE_FIELD_NAMES);
        return Collections.unmodifiableList(_propertyDescriptors);
    }

    @Override
    public HDFSRecordWriter createHDFSRecordWriter(final ProcessContext context, final FlowFile flowFile, final Configuration conf, final Path path, final RecordSchema schema)
            throws IOException, SchemaNotFoundException {

        final long stripeSize = context.getProperty(STRIPE_SIZE).asDataSize(DataUnit.B).longValue();
        final int bufferSize = context.getProperty(BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
        final CompressionKind compressionType = CompressionKind.valueOf(context.getProperty(COMPRESSION_TYPE).getValue());
        final boolean normalizeForHive = context.getProperty(HIVE_FIELD_NAMES).asBoolean();
        TypeInfo orcSchema = NiFiOrcUtils.getOrcSchema(schema, normalizeForHive);
        final Writer orcWriter = NiFiOrcUtils.createWriter(path, conf, orcSchema, stripeSize, compressionType, bufferSize);
        final String hiveTableName = context.getProperty(HIVE_TABLE_NAME).isSet()
                ? context.getProperty(HIVE_TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue()
                : NiFiOrcUtils.normalizeHiveTableName(schema.getIdentifier().getName().orElse("unknown"));
        final boolean hiveFieldNames = context.getProperty(HIVE_FIELD_NAMES).asBoolean();

        return new ORCHDFSRecordWriter(orcWriter, schema, hiveTableName, hiveFieldNames);
    }
}
