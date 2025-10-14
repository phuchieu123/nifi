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

package org.apache.nifi.csv;

import org.apache.commons.csv.CSVFormat;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaAccessStrategy;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schema.inference.InferSchemaAccessStrategy;
import org.apache.nifi.schema.inference.RecordSourceFactory;
import org.apache.nifi.schema.inference.SchemaInferenceEngine;
import org.apache.nifi.schema.inference.SchemaInferenceUtil;
import org.apache.nifi.schema.inference.TimeValueInference;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.DateTimeUtils;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SchemaRegistryService;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.stream.io.NonCloseableInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Tags({"csv", "parse", "record", "row", "reader", "delimited", "comma", "separated", "values"})
@CapabilityDescription("Phân tích dữ liệu định dạng CSV, trả về mỗi dòng trong file CSV như một bản ghi riêng biệt. "
    + "Reader này cho phép suy luận schema dựa trên dòng đầu tiên của CSV, nếu có 'dòng header', hoặc cung cấp một schema rõ ràng "
    + "để diễn giải các giá trị. Xem phần Sử dụng Controller Service để biết thêm tài liệu.")
public class CSVReader extends SchemaRegistryService implements RecordReaderFactory {

    private static final AllowableValue HEADER_DERIVED = new AllowableValue("csv-header-derived", "Sử dụng trường từ Header",
        "Dòng đầu tiên không phải comment trong file CSV là dòng header chứa tên các cột. Schema sẽ được suy luận bằng cách sử dụng "
            + "tên các cột trong header và giả định rằng tất cả các cột đều có kiểu String.");

    // CSV parsers
    public static final AllowableValue APACHE_COMMONS_CSV = new AllowableValue("commons-csv", "Apache Commons CSV",
            "Triển khai parser CSV từ thư viện Apache Commons CSV.");

    public static final AllowableValue JACKSON_CSV = new AllowableValue("jackson-csv", "Jackson CSV",
            "Triển khai parser CSV từ thư viện Jackson Dataformats.");


    public static final PropertyDescriptor CSV_PARSER = new PropertyDescriptor.Builder()
            .name("csv-reader-csv-parser")
            .displayName("CSV Parser")
            .description("Xác định parser nào được sử dụng để đọc các bản ghi CSV. LƯU Ý: Các parser khác nhau có thể hỗ trợ các tập chức năng khác nhau "
                    + "và cũng có thể có hiệu suất khác nhau.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues(APACHE_COMMONS_CSV, JACKSON_CSV)
            .defaultValue(APACHE_COMMONS_CSV.getValue())
            .required(true)
            .build();

    public static final PropertyDescriptor TRIM_DOUBLE_QUOTE = new PropertyDescriptor.Builder()
            .name("Trim double quote")
            .description("Xác định có cắt bỏ dấu ngoặc kép đầu và cuối hay không. Ví dụ: với trim, chuỗi '\"test\"'"
                    +" sẽ được phân tích thành 'test', nếu không trim sẽ được phân tích thành '\"test\"'."
                    + "Nếu đặt là 'false', có nghĩa là tuân thủ đầy đủ RFC-4180. Giá trị mặc định là true, tức có trim.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("true", "false")
            .defaultValue("true")
            .dependsOn(CSVUtils.CSV_FORMAT, CSVUtils.RFC_4180)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .required(true)
            .build();

    private volatile ConfigurationContext context;

    private volatile String csvParser;
    private volatile String dateFormat;
    private volatile String timeFormat;
    private volatile String timestampFormat;
    private volatile boolean firstLineIsHeader;
    private volatile boolean ignoreHeader;
    private volatile String charSet;

    // it will be initialized only if there are no dynamic csv formatting properties
    private volatile CSVFormat csvFormat;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(CSV_PARSER);
        properties.add(DateTimeUtils.DATE_FORMAT);
        properties.add(DateTimeUtils.TIME_FORMAT);
        properties.add(DateTimeUtils.TIMESTAMP_FORMAT);
        properties.add(CSVUtils.CSV_FORMAT);
        properties.add(CSVUtils.VALUE_SEPARATOR);
        properties.add(CSVUtils.RECORD_SEPARATOR);
        properties.add(CSVUtils.FIRST_LINE_IS_HEADER);
        properties.add(CSVUtils.IGNORE_CSV_HEADER);
        properties.add(CSVUtils.QUOTE_CHAR);
        properties.add(CSVUtils.ESCAPE_CHAR);
        properties.add(CSVUtils.COMMENT_MARKER);
        properties.add(CSVUtils.NULL_STRING);
        properties.add(CSVUtils.TRIM_FIELDS);
        properties.add(CSVUtils.CHARSET);
        properties.add(CSVUtils.ALLOW_DUPLICATE_HEADER_NAMES);
        properties.add(TRIM_DOUBLE_QUOTE);
        return properties;
    }

    @OnEnabled
    public void storeStaticProperties(final ConfigurationContext context) {
        this.context = context;

        this.csvParser = context.getProperty(CSV_PARSER).getValue();
        this.dateFormat = context.getProperty(DateTimeUtils.DATE_FORMAT).getValue();
        this.timeFormat = context.getProperty(DateTimeUtils.TIME_FORMAT).getValue();
        this.timestampFormat = context.getProperty(DateTimeUtils.TIMESTAMP_FORMAT).getValue();
        this.firstLineIsHeader = context.getProperty(CSVUtils.FIRST_LINE_IS_HEADER).asBoolean();
        this.ignoreHeader = context.getProperty(CSVUtils.IGNORE_CSV_HEADER).asBoolean();
        this.charSet = context.getProperty(CSVUtils.CHARSET).getValue();

        // Ensure that if we are deriving schema from header that we always treat the first line as a header,
        // regardless of the 'First Line is Header' property
        final String accessStrategy = context.getProperty(SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY).getValue();
        if (HEADER_DERIVED.getValue().equals(accessStrategy) || SchemaInferenceUtil.INFER_SCHEMA.getValue().equals(accessStrategy)) {
            this.firstLineIsHeader = true;
        }

        if (!CSVUtils.isDynamicCSVFormat(context)) {
            this.csvFormat = CSVUtils.createCSVFormat(context, Collections.emptyMap());
        } else {
            this.csvFormat = null;
        }
    }

    @Override
    public RecordReader createRecordReader(final Map<String, String> variables, final InputStream in, final long inputLength, final ComponentLog logger) throws IOException, SchemaNotFoundException {
        // Use Mark/Reset of a BufferedInputStream in case we read from the Input Stream for the header.
        in.mark(1024 * 1024);
        final RecordSchema schema = getSchema(variables, new NonCloseableInputStream(in), null);
        in.reset();

        final CSVFormat format;
        if (this.csvFormat != null) {
            format = this.csvFormat;
        } else {
            format = CSVUtils.createCSVFormat(context, variables);
        }

        final boolean trimDoubleQuote = context.getProperty(TRIM_DOUBLE_QUOTE).asBoolean();

        if (APACHE_COMMONS_CSV.getValue().equals(csvParser)) {
            return new CSVRecordReader(in, logger, schema, format, firstLineIsHeader, ignoreHeader, dateFormat, timeFormat, timestampFormat, charSet, trimDoubleQuote);
        } else if (JACKSON_CSV.getValue().equals(csvParser)) {
            return new JacksonCSVRecordReader(in, logger, schema, format, firstLineIsHeader, ignoreHeader, dateFormat, timeFormat, timestampFormat, charSet, trimDoubleQuote);
        } else {
            throw new IOException("Parser not supported");
        }
    }

    @Override
    protected SchemaAccessStrategy getSchemaAccessStrategy(final String allowableValue, final SchemaRegistry schemaRegistry, final PropertyContext context) {
        if (allowableValue.equalsIgnoreCase(HEADER_DERIVED.getValue())) {
            return new CSVHeaderSchemaStrategy(context);
        } else if (allowableValue.equalsIgnoreCase(SchemaInferenceUtil.INFER_SCHEMA.getValue())) {
            final RecordSourceFactory<CSVRecordAndFieldNames> sourceFactory = (variables, in) -> new CSVRecordSource(in, context, variables);
            final SchemaInferenceEngine<CSVRecordAndFieldNames> inference = new CSVSchemaInference(new TimeValueInference(dateFormat, timeFormat, timestampFormat));
            return new InferSchemaAccessStrategy<>(sourceFactory, inference, getLogger());
        }

        return super.getSchemaAccessStrategy(allowableValue, schemaRegistry, context);
    }

    @Override
    protected List<AllowableValue> getSchemaAccessStrategyValues() {
        final List<AllowableValue> allowableValues = new ArrayList<>(super.getSchemaAccessStrategyValues());
        allowableValues.add(HEADER_DERIVED);
        allowableValues.add(SchemaInferenceUtil.INFER_SCHEMA);
        return allowableValues;
    }

    @Override
    protected AllowableValue getDefaultSchemaAccessStrategy() {
        return SchemaInferenceUtil.INFER_SCHEMA;
    }
}
