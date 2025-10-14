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

package org.apache.nifi.syslog;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaAccessStrategy;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SchemaRegistryService;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.serialization.record.StandardSchemaIdentifier;
import org.apache.nifi.syslog.attributes.SyslogAttributes;
import org.apache.nifi.syslog.parsers.SyslogParser;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Tags({"syslog", "logs", "logfiles", "parse", "text", "record", "reader"})
@CapabilityDescription("Cố gắng phân tích cú pháp nội dung của một tin nhắn Syslog theo RFC5424 và RFC3164. Trong " +
        "trường hợp các tin nhắn được định dạng RFC5424, dữ liệu có cấu trúc không được hỗ trợ và sẽ được trả về như một phần của tin nhắn." +
        "Lưu ý: Hãy nhớ rằng RFC3164 chỉ mang tính thông tin và có rất nhiều cách triển khai khác nhau trong" +
        " thực tế.")
public class SyslogReader extends SchemaRegistryService implements RecordReaderFactory {

    public static final String GENERIC_SYSLOG_SCHEMA_NAME = "default-syslog-schema";
    static final AllowableValue GENERIC_SYSLOG_SCHEMA = new AllowableValue(GENERIC_SYSLOG_SCHEMA_NAME, "Sử dụng Lược đồ Syslog Chung",
            "Lược đồ sẽ là lược đồ Syslog mặc định.");

    static final String RAW_MESSAGE_NAME = "_raw";

    public static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("Bộ ký tự")
            .description("Chỉ định bộ ký tự của các tin nhắn Syslog")
            .required(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();
    public static final PropertyDescriptor ADD_RAW = new PropertyDescriptor.Builder()
            .displayName("Tin nhắn thô")
            .name("syslog-5424-reader-raw-message")
            .description("Nếu đúng, bản ghi sẽ có một trường " + RAW_MESSAGE_NAME + " chứa tin nhắn thô")
            .required(true)
            .defaultValue("false")
            .allowableValues("true", "false")
            .build();
    private volatile SyslogParser parser;
    private volatile static boolean includeRaw;
    private volatile RecordSchema recordSchema;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(2);
        properties.add(CHARSET);
        properties.add(ADD_RAW);
        return properties;
    }


    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        final String charsetName = context.getProperty(CHARSET).getValue();
        parser = new SyslogParser(Charset.forName(charsetName));
        includeRaw = context.getProperty(ADD_RAW).asBoolean();
        recordSchema = createRecordSchema();
    }

    @Override
    protected List<AllowableValue> getSchemaAccessStrategyValues() {
        final List<AllowableValue> allowableValues = new ArrayList<>();
        allowableValues.add(GENERIC_SYSLOG_SCHEMA);
        return allowableValues;
    }

    @Override
    protected AllowableValue getDefaultSchemaAccessStrategy() {
        return GENERIC_SYSLOG_SCHEMA;
    }

    @Override
    protected SchemaAccessStrategy getSchemaAccessStrategy(final String strategy, final SchemaRegistry schemaRegistry, final PropertyContext context) {
        return createAccessStrategy();
    }

    static RecordSchema createRecordSchema() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField(SyslogAttributes.PRIORITY.key(), RecordFieldType.STRING.getDataType(), true));
        fields.add(new RecordField(SyslogAttributes.SEVERITY.key(), RecordFieldType.STRING.getDataType(), true));
        fields.add(new RecordField(SyslogAttributes.FACILITY.key(), RecordFieldType.STRING.getDataType(), true));
        fields.add(new RecordField(SyslogAttributes.VERSION.key(), RecordFieldType.STRING.getDataType(), true));
        fields.add(new RecordField(SyslogAttributes.TIMESTAMP.key(), RecordFieldType.STRING.getDataType(), true));
        fields.add(new RecordField(SyslogAttributes.HOSTNAME.key(), RecordFieldType.STRING.getDataType(), true));
        fields.add(new RecordField(SyslogAttributes.BODY.key(), RecordFieldType.STRING.getDataType(), true));

        if(includeRaw) {
            fields.add(new RecordField(RAW_MESSAGE_NAME, RecordFieldType.STRING.getDataType(), true));
        }

        SchemaIdentifier schemaIdentifier = new StandardSchemaIdentifier.Builder().name(GENERIC_SYSLOG_SCHEMA_NAME).build();
        final RecordSchema schema = new SimpleRecordSchema(fields,schemaIdentifier);
        return schema;
    }

    private SchemaAccessStrategy createAccessStrategy() {
        return new SchemaAccessStrategy() {
            private final Set<SchemaField> schemaFields = EnumSet.noneOf(SchemaField.class);

            @Override
            public RecordSchema getSchema(Map<String, String> variables, InputStream contentStream, RecordSchema readSchema) {
                return recordSchema;
            }

            @Override
            public Set<SchemaField> getSuppliedSchemaFields() {
                return schemaFields;
            }
        };
    }

    @Override
    public RecordReader createRecordReader(final Map<String, String> variables, final InputStream in, final long inputLength, final ComponentLog logger) throws IOException, SchemaNotFoundException {
        final RecordSchema schema = getSchema(variables, in, null);
        return new SyslogRecordReader(parser, includeRaw, in, schema);
    }
}
