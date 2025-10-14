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

package org.apache.nifi.xml;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaAccessStrategy;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schema.inference.SchemaInferenceEngine;
import org.apache.nifi.schema.inference.RecordSourceFactory;
import org.apache.nifi.schema.inference.SchemaInferenceUtil;
import org.apache.nifi.schema.inference.TimeValueInference;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.DateTimeUtils;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SchemaRegistryService;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.xml.inference.XmlNode;
import org.apache.nifi.xml.inference.XmlRecordSource;
import org.apache.nifi.xml.inference.XmlSchemaInference;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.nifi.schema.inference.SchemaInferenceUtil.INFER_SCHEMA;

@Tags({"xml", "record", "reader", "parser"})
@CapabilityDescription("Đọc nội dung XML và tạo các đối tượng Record. Các bản ghi được mong đợi ở cấp thứ hai của " +
        "dữ liệu XML, được nhúng trong một thẻ gốc bao quanh.")
public class XMLReader extends SchemaRegistryService implements RecordReaderFactory {

    public static final AllowableValue RECORD_SINGLE = new AllowableValue("false", "false",
        "Mỗi FlowFile sẽ bao gồm một bản ghi duy nhất mà không có bất kỳ loại \"wrapper\" nào.");
    public static final AllowableValue RECORD_ARRAY = new AllowableValue("true", "true",
        "Mỗi FlowFile sẽ bao gồm không hoặc nhiều bản ghi. Phần tử XML ngoài cùng nhất được mong đợi là một \"wrapper\" và sẽ bị bỏ qua.");
    public static final AllowableValue RECORD_EVALUATE = new AllowableValue("${xml.stream.is.array}", "Sử dụng thuộc tính 'xml.stream.is.array'",
        "Việc coi một FlowFile là một Bản ghi duy nhất hay một mảng gồm nhiều Bản ghi được xác định bởi giá trị của thuộc tính 'xml.stream.is.array'. "
            + "Nếu giá trị của thuộc tính là 'true' (không phân biệt chữ hoa chữ thường), thì XML Reader sẽ coi FlowFile là một chuỗi các Bản ghi với phần tử bên ngoài bị bỏ qua. "
            + "Nếu giá trị của thuộc tính là 'false' (không phân biệt chữ hoa chữ thường), thì FlowFile được coi là một Bản ghi duy nhất và không giả định có phần tử bao bọc. "
            + "Nếu thuộc tính bị thiếu hoặc giá trị của nó là bất kỳ thứ gì khác ngoài 'true' hoặc 'false', thì một Exception sẽ được ném ra và không có bản ghi nào được phân tích cú pháp.");

    public static final PropertyDescriptor RECORD_FORMAT = new PropertyDescriptor.Builder()
            .name("record_format")
            .displayName("Mong đợi các bản ghi dưới dạng mảng")
            .description("Thuộc tính này xác định xem trình đọc có mong đợi một FlowFile bao gồm một Bản ghi duy nhất hay một chuỗi các Bản ghi với một \"phần tử bao bọc\" hay không. Bởi vì XML không "
                + "cung cấp cách đọc trực tiếp một chuỗi tài liệu XML từ một luồng, nên thông thường người ta kết hợp nhiều tài liệu XML bằng cách nối chúng lại với nhau và sau đó bao bọc toàn bộ "
                + "khối XML bằng một \"phần tử bao bọc\". Thuộc tính này quy định liệu trình đọc có mong đợi một FlowFile bao gồm một Bản ghi duy nhất hay một chuỗi các Bản ghi với một \"phần tử bao bọc\" "
                + "sẽ bị bỏ qua hay không.")
            .allowableValues(RECORD_SINGLE, RECORD_ARRAY, RECORD_EVALUATE)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue(RECORD_SINGLE.getValue())
            .required(true)
            .build();

    public static final PropertyDescriptor ATTRIBUTE_PREFIX = new PropertyDescriptor.Builder()
            .name("attribute_prefix")
            .displayName("Tiền tố thuộc tính")
            .description("Nếu thuộc tính này được đặt, tên của các thuộc tính sẽ được thêm một tiền tố vào trước khi chúng được thêm vào một bản ghi.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .build();

    public static final PropertyDescriptor CONTENT_FIELD_NAME = new PropertyDescriptor.Builder()
            .name("content_field_name")
            .displayName("Tên trường cho nội dung")
            .description("Nếu các thẻ có nội dung (ví dụ: <field>content</field>) được định nghĩa là các bản ghi lồng nhau trong lược đồ, " +
                    "tên của thẻ sẽ được sử dụng làm tên cho bản ghi và giá trị của thuộc tính này sẽ được sử dụng làm tên cho trường. " +
                    "Nếu các thẻ có nội dung sẽ được phân tích cú pháp cùng với các thuộc tính (ví dụ: <field attribute=\"123\">content</field>), " +
                    "chúng phải được định nghĩa là các bản ghi. Trong trường hợp như vậy, tên của thẻ sẽ được sử dụng làm tên cho bản ghi và " +
                    "giá trị của thuộc tính này sẽ được sử dụng làm tên cho trường chứa nội dung gốc. Tên của thuộc tính " +
                    "sẽ được sử dụng để tạo một trường bản ghi mới, nội dung của trường đó sẽ là giá trị của thuộc tính. " +
                    "Để biết thêm thông tin, hãy xem phần 'Chi tiết bổ sung...' trong tài liệu về dịch vụ controller XMLReader.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .build();

    public static final PropertyDescriptor PARSE_XML_ATTRIBUTES = new PropertyDescriptor.Builder()
            .name("parse_xml_attributes")
            .displayName("Phân tích cú pháp thuộc tính XML")
            .description("Khi 'Chiến lược truy cập lược đồ' là 'Suy ra lược đồ' và thuộc tính này là 'true' thì các thuộc tính XML được phân tích cú pháp và " +
                    "được thêm vào bản ghi dưới dạng các trường mới. Khi lược đồ được suy ra nhưng thuộc tính này là 'false', " +
                    "các thuộc tính XML và giá trị của chúng sẽ bị bỏ qua.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(false)
            .dependsOn(SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, INFER_SCHEMA)
            .build();

    private volatile boolean parseXmlAttributes;
    private volatile String dateFormat;
    private volatile String timeFormat;
    private volatile String timestampFormat;

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        this.parseXmlAttributes = context.getProperty(PARSE_XML_ATTRIBUTES).asBoolean();
        this.dateFormat = context.getProperty(DateTimeUtils.DATE_FORMAT).getValue();
        this.timeFormat = context.getProperty(DateTimeUtils.TIME_FORMAT).getValue();
        this.timestampFormat = context.getProperty(DateTimeUtils.TIMESTAMP_FORMAT).getValue();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(PARSE_XML_ATTRIBUTES);
        properties.add(SchemaInferenceUtil.SCHEMA_CACHE);
        properties.add(RECORD_FORMAT);
        properties.add(ATTRIBUTE_PREFIX);
        properties.add(CONTENT_FIELD_NAME);
        properties.add(DateTimeUtils.DATE_FORMAT);
        properties.add(DateTimeUtils.TIME_FORMAT);
        properties.add(DateTimeUtils.TIMESTAMP_FORMAT);
        return properties;
    }

    @Override
    protected List<AllowableValue> getSchemaAccessStrategyValues() {
        final List<AllowableValue> allowableValues = new ArrayList<>(super.getSchemaAccessStrategyValues());
        allowableValues.add(INFER_SCHEMA);
        return allowableValues;
    }

    @Override
    protected SchemaAccessStrategy getSchemaAccessStrategy(final String strategy, final SchemaRegistry schemaRegistry, final PropertyContext context) {

        final RecordSourceFactory<XmlNode> sourceFactory = (variables, contentStream) -> {
            String contentFieldName = trim(context.getProperty(CONTENT_FIELD_NAME).evaluateAttributeExpressions(variables).getValue());
            contentFieldName = (contentFieldName == null) ? "value" : contentFieldName;
            return new XmlRecordSource(contentStream, contentFieldName, isMultipleRecords(context, variables), parseXmlAttributes);
        };
        final Supplier<SchemaInferenceEngine<XmlNode>> schemaInference = () -> new XmlSchemaInference(new TimeValueInference(dateFormat, timeFormat, timestampFormat));

        return SchemaInferenceUtil.getSchemaAccessStrategy(strategy, context, getLogger(), sourceFactory, schemaInference,
            () -> super.getSchemaAccessStrategy(strategy, schemaRegistry, context));
    }

    private boolean isMultipleRecords(final PropertyContext context, final Map<String, String> variables) {
        final String recordFormat = context.getProperty(RECORD_FORMAT).evaluateAttributeExpressions(variables).getValue().trim();
        if ("true".equalsIgnoreCase(recordFormat)) {
            return true;
        } else if ("false".equalsIgnoreCase(recordFormat)) {
            return false;
        } else {
            throw new ProcessException("Cannot parse XML Records because the '" + RECORD_FORMAT.getDisplayName() + "' property evaluates to '"
                + recordFormat + "', which is neither 'true' nor 'false'");
        }
    }

    @Override
    protected AllowableValue getDefaultSchemaAccessStrategy() {
        return INFER_SCHEMA;
    }

    @Override
    public RecordReader createRecordReader(final Map<String, String> variables, final InputStream in, final long inputLength, final ComponentLog logger)
            throws IOException, SchemaNotFoundException, MalformedRecordException {
        final ConfigurationContext context = getConfigurationContext();

        final RecordSchema schema = getSchema(variables, in, null);

        final String attributePrefix = trim(context.getProperty(ATTRIBUTE_PREFIX).evaluateAttributeExpressions(variables).getValue());
        final String contentFieldName = trim(context.getProperty(CONTENT_FIELD_NAME).evaluateAttributeExpressions(variables).getValue());
        final boolean isArray = isMultipleRecords(context, variables);

        return new XMLRecordReader(in, schema, isArray, parseXmlAttributes, attributePrefix, contentFieldName, dateFormat, timeFormat, timestampFormat, logger);
    }

    private String trim(final String value) {
        return value == null ? null : value.trim();
    }
}
