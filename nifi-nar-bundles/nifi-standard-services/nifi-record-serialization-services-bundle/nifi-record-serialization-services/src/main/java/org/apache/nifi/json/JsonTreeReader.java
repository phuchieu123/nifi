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

package org.apache.nifi.json;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaAccessStrategy;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schema.inference.RecordSourceFactory;
import org.apache.nifi.schema.inference.SchemaInferenceEngine;
import org.apache.nifi.schema.inference.SchemaInferenceUtil;
import org.apache.nifi.schema.inference.TimeValueInference;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.DateTimeUtils;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SchemaRegistryService;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.nifi.schema.access.SchemaAccessUtils.CONFLUENT_ENCODED_SCHEMA;
import static org.apache.nifi.schema.access.SchemaAccessUtils.HWX_CONTENT_ENCODED_SCHEMA;
import static org.apache.nifi.schema.access.SchemaAccessUtils.HWX_SCHEMA_REF_ATTRIBUTES;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_NAME_PROPERTY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_TEXT_PROPERTY;
import static org.apache.nifi.schema.inference.SchemaInferenceUtil.INFER_SCHEMA;
import static org.apache.nifi.schema.inference.SchemaInferenceUtil.SCHEMA_CACHE;

@Tags({"json", "tree", "record", "reader", "parser"})
@CapabilityDescription("Phân tích JSON thành các đối tượng Record riêng lẻ. Reader mong đợi mỗi record là JSON hợp lệ, "
        + "nhưng nội dung của FlowFile có thể bao gồm nhiều record, mỗi record là một mảng JSON hoặc đối tượng JSON hợp lệ "
        + "với khoảng trắng tùy chọn giữa chúng, ví dụ định dạng 'JSON-per-line' phổ biến. "
        + "Nếu gặp mảng, mỗi phần tử trong mảng sẽ được coi là một record riêng. "
        + "Nếu schema được cấu hình có một trường không có trong JSON, giá trị null sẽ được sử dụng. Nếu JSON có một trường "
        + "không có trong schema, trường đó sẽ bị bỏ qua. "
        + "Xem phần Usage của Controller Service để biết thêm thông tin và ví dụ.")
@SeeAlso(JsonPathReader.class)
public class JsonTreeReader extends SchemaRegistryService implements RecordReaderFactory {
    protected volatile String dateFormat;
    protected volatile String timeFormat;
    protected volatile String timestampFormat;
    protected volatile String startingFieldName;
    protected volatile StartingFieldStrategy startingFieldStrategy;
    protected volatile SchemaApplicationStrategy schemaApplicationStrategy;
    protected volatile StreamReadConstraints streamReadConstraints;
    private volatile boolean allowComments;

    public static final PropertyDescriptor STARTING_FIELD_STRATEGY = new PropertyDescriptor.Builder()
            .name("starting-field-strategy")
            .displayName("Chiến lược trường bắt đầu")
            .description("Bắt đầu xử lý từ node gốc hoặc từ một node lồng nhau được chỉ định.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue(StartingFieldStrategy.ROOT_NODE.getValue())
            .allowableValues(StartingFieldStrategy.class)
            .build();

    public static final PropertyDescriptor STARTING_FIELD_NAME = new PropertyDescriptor.Builder()
            .name("starting-field-name")
            .displayName("Tên trường bắt đầu")
            .description("Bỏ qua tới trường JSON lồng nhau (mảng hoặc đối tượng) được chỉ định để bắt đầu xử lý.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue(null)
            .dependsOn(STARTING_FIELD_STRATEGY, StartingFieldStrategy.NESTED_FIELD.name())
            .build();

    public static final PropertyDescriptor SCHEMA_APPLICATION_STRATEGY = new PropertyDescriptor.Builder()
            .name("schema-application-strategy")
            .displayName("Chiến lược áp dụng schema")
            .description("Xác định liệu schema được định nghĩa cho toàn bộ JSON hay chỉ cho phần được chọn bắt đầu từ \"Tên trường bắt đầu\".")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue(SchemaApplicationStrategy.SELECTED_PART.getValue())
            .dependsOn(STARTING_FIELD_STRATEGY, StartingFieldStrategy.NESTED_FIELD.name())
            .dependsOn(SCHEMA_ACCESS_STRATEGY, SCHEMA_NAME_PROPERTY, SCHEMA_TEXT_PROPERTY, HWX_SCHEMA_REF_ATTRIBUTES, HWX_CONTENT_ENCODED_SCHEMA, CONFLUENT_ENCODED_SCHEMA)
            .allowableValues(SchemaApplicationStrategy.class)
            .build();


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(new PropertyDescriptor.Builder()
                .fromPropertyDescriptor(SCHEMA_CACHE)
                .dependsOn(SCHEMA_ACCESS_STRATEGY, INFER_SCHEMA)
                .build());
        properties.add(STARTING_FIELD_STRATEGY);
        properties.add(STARTING_FIELD_NAME);
        properties.add(SCHEMA_APPLICATION_STRATEGY);
        properties.add(AbstractJsonRowRecordReader.MAX_STRING_LENGTH);
        properties.add(AbstractJsonRowRecordReader.ALLOW_COMMENTS);
        properties.add(DateTimeUtils.DATE_FORMAT);
        properties.add(DateTimeUtils.TIME_FORMAT);
        properties.add(DateTimeUtils.TIMESTAMP_FORMAT);
        return properties;
    }

    @OnEnabled
    public void storePropertyValues(final ConfigurationContext context) {
        this.dateFormat = context.getProperty(DateTimeUtils.DATE_FORMAT).getValue();
        this.timeFormat = context.getProperty(DateTimeUtils.TIME_FORMAT).getValue();
        this.timestampFormat = context.getProperty(DateTimeUtils.TIMESTAMP_FORMAT).getValue();
        this.startingFieldStrategy = StartingFieldStrategy.valueOf(context.getProperty(STARTING_FIELD_STRATEGY).getValue());
        this.startingFieldName = context.getProperty(STARTING_FIELD_NAME).getValue();
        this.schemaApplicationStrategy = SchemaApplicationStrategy.valueOf(context.getProperty(SCHEMA_APPLICATION_STRATEGY).getValue());
        this.streamReadConstraints = buildStreamReadConstraints(context);
        this.allowComments = isAllowCommentsEnabled(context);
    }

    /**
     * Build Stream Read Constraints based on available properties
     *
     * @param context Configuration Context with property values
     * @return Stream Read Constraints
     */
    protected StreamReadConstraints buildStreamReadConstraints(final ConfigurationContext context) {
        final int maxStringLength = context.getProperty(AbstractJsonRowRecordReader.MAX_STRING_LENGTH).asDataSize(DataUnit.B).intValue();
        return StreamReadConstraints.builder().maxStringLength(maxStringLength).build();
    }

    /**
     * Determine whether to allow comments when parsing based on available properties
     *
     * @param context Configuration Context with property values
     * @return Allow comments status
     */
    protected boolean isAllowCommentsEnabled(final ConfigurationContext context) {
        return context.getProperty(AbstractJsonRowRecordReader.ALLOW_COMMENTS).asBoolean();
    }

    @Override
    protected List<AllowableValue> getSchemaAccessStrategyValues() {
        final List<AllowableValue> allowableValues = new ArrayList<>();
        allowableValues.add(INFER_SCHEMA);
        allowableValues.addAll(super.getSchemaAccessStrategyValues());
        return allowableValues;
    }

    @Override
    protected SchemaAccessStrategy getSchemaAccessStrategy(final String schemaAccessStrategy, final SchemaRegistry schemaRegistry, final PropertyContext context) {
        final RecordSourceFactory<JsonNode> jsonSourceFactory = createJsonRecordSourceFactory();
        final Supplier<SchemaInferenceEngine<JsonNode>> inferenceSupplier =
                () -> new JsonSchemaInference(new TimeValueInference(dateFormat, timeFormat, timestampFormat));

        return SchemaInferenceUtil.getSchemaAccessStrategy(schemaAccessStrategy, context, getLogger(), jsonSourceFactory, inferenceSupplier,
                () -> super.getSchemaAccessStrategy(schemaAccessStrategy, schemaRegistry, context));
    }

    protected RecordSourceFactory<JsonNode> createJsonRecordSourceFactory() {
        return (variables, in) -> new JsonRecordSource(in, startingFieldStrategy, startingFieldName, streamReadConstraints);
    }

    @Override
    protected AllowableValue getDefaultSchemaAccessStrategy() {
        return INFER_SCHEMA;
    }

    public RecordReader createRecordReader(final Map<String, String> variables, final InputStream in, final long inputLength, final ComponentLog logger)
            throws IOException, MalformedRecordException, SchemaNotFoundException {
        final RecordSchema schema = getSchema(variables, in, null);
        return createJsonTreeRowRecordReader(in, logger, schema);
    }

    protected JsonTreeRowRecordReader createJsonTreeRowRecordReader(final InputStream in, final ComponentLog logger, final RecordSchema schema) throws IOException, MalformedRecordException {
        return new JsonTreeRowRecordReader(in, logger, schema, dateFormat, timeFormat, timestampFormat, startingFieldStrategy, startingFieldName,
                schemaApplicationStrategy, null, allowComments, streamReadConstraints, new JsonParserFactory());
    }
}
