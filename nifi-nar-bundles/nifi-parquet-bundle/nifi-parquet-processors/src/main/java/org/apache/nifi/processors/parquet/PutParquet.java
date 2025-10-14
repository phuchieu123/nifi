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
package org.apache.nifi.processors.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.parquet.utils.ParquetConfig;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processors.hadoop.AbstractPutHDFSRecord;
import org.apache.nifi.processors.hadoop.record.HDFSRecordWriter;
import org.apache.nifi.parquet.hadoop.AvroParquetHDFSRecordWriter;
import org.apache.nifi.parquet.utils.ParquetUtils;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.nifi.parquet.utils.ParquetUtils.createParquetConfig;
import static org.apache.nifi.parquet.utils.ParquetUtils.applyCommonConfig;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"put", "parquet", "hadoop", "HDFS", "filesystem", "record"})
@CapabilityDescription("Đọc các bản ghi từ FlowFile đến sử dụng Record Reader được cung cấp, và ghi các bản ghi đó "
        + "vào file Parquet. Sơ đồ cho file Parquet phải được cung cấp trong thuộc tính của processor. Processor này "
        + "sẽ ghi trước vào một file tạm (.dot) và sau khi ghi thành công tất cả bản ghi, file .dot sẽ được đổi tên "
        + "thành tên cuối cùng. Nếu không thể đổi tên file .dot, sẽ thử đổi tên tối đa 10 lần, nếu vẫn không thành công, "
        + "file .dot sẽ bị xóa và FlowFile sẽ được chuyển tới failure. "
        + "Nếu có lỗi xảy ra khi đọc bản ghi đầu vào hoặc ghi bản ghi ra đầu ra, toàn bộ file .dot sẽ bị xóa và FlowFile "
        + "sẽ được chuyển tới failure hoặc retry tùy theo lỗi.")
@ReadsAttribute(attribute = "filename", description = "Tên file ghi ra lấy từ giá trị của attribute này.")
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "Tên file được lưu trong attribute này."),
        @WritesAttribute(attribute = "absolute.hdfs.path", description = "Đường dẫn tuyệt đối tới file được lưu trong attribute này."),
        @WritesAttribute(attribute = "hadoop.file.url", description = "URL Hadoop của file được lưu trong attribute này."),
        @WritesAttribute(attribute = "record.count", description = "Số lượng bản ghi đã ghi vào file Parquet")
})
@Restricted(restrictions = {
    @Restriction(
        requiredPermission = RequiredPermission.WRITE_DISTRIBUTED_FILESYSTEM,
        explanation = "Cho phép operator ghi bất kỳ file nào mà NiFi có quyền truy cập trong HDFS hoặc hệ thống file cục bộ.")
})
public class PutParquet extends AbstractPutHDFSRecord {

    public static final PropertyDescriptor REMOVE_CRC_FILES = new PropertyDescriptor.Builder()
            .name("remove-crc-files")
            .displayName("Xóa file CRC")
            .description("Chỉ định có xóa file CRC tương ứng sau khi ghi thành công file Parquet hay không")
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    @Override
    public List<AllowableValue> getCompressionTypes(final ProcessorInitializationContext context) {
        return ParquetUtils.COMPRESSION_TYPES;
    }

    @Override
    public String getDefaultCompressionType(final ProcessorInitializationContext context) {
        return CompressionCodecName.UNCOMPRESSED.name();
    }

    @Override
    public List<PropertyDescriptor> getAdditionalProperties() {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(ParquetUtils.ROW_GROUP_SIZE);
        props.add(ParquetUtils.PAGE_SIZE);
        props.add(ParquetUtils.DICTIONARY_PAGE_SIZE);
        props.add(ParquetUtils.MAX_PADDING_SIZE);
        props.add(ParquetUtils.ENABLE_DICTIONARY_ENCODING);
        props.add(ParquetUtils.ENABLE_VALIDATION);
        props.add(ParquetUtils.WRITER_VERSION);
        props.add(ParquetUtils.AVRO_WRITE_OLD_LIST_STRUCTURE);
        props.add(ParquetUtils.AVRO_ADD_LIST_ELEMENT_RECORDS);
        props.add(REMOVE_CRC_FILES);
        return Collections.unmodifiableList(props);
    }

    @Override
    public HDFSRecordWriter createHDFSRecordWriter(final ProcessContext context, final FlowFile flowFile, final Configuration conf, final Path path, final RecordSchema schema)
            throws IOException, SchemaNotFoundException {

        final Schema avroSchema = AvroTypeUtil.extractAvroSchema(schema);

        final AvroParquetWriter.Builder<GenericRecord> parquetWriter = AvroParquetWriter
                .<GenericRecord>builder(path)
                .withSchema(avroSchema);

        final ParquetConfig parquetConfig = createParquetConfig(context, flowFile.getAttributes());
        applyCommonConfig(parquetWriter, conf, parquetConfig);

        return new AvroParquetHDFSRecordWriter(parquetWriter.build(), avroSchema);
    }

    @Override
    protected FlowFile postProcess(final ProcessContext context, final ProcessSession session, final FlowFile flowFile, final Path destFile) {
        final boolean removeCRCFiles = context.getProperty(REMOVE_CRC_FILES).asBoolean();
        if (removeCRCFiles) {
            final String filename = destFile.getName();
            final String hdfsPath = destFile.getParent().toString();

            final Path crcFile = new Path(hdfsPath, "." + filename + ".crc");
            deleteQuietly(getFileSystem(), crcFile);
        }

        return flowFile;
    }
}
