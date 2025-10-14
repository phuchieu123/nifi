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
package org.apache.nifi.excel;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.DescribedValue;
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
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SchemaRegistryService;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.stream.io.NonCloseableInputStream;
import org.apache.poi.ss.usermodel.Row;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Tags({"excel", "spreadsheet", "xlsx", "parse", "record", "row", "reader", "values", "cell"})
@CapabilityDescription("Phân tích một tài liệu Microsoft Excel, trả về mỗi dòng trong mỗi sheet như một bản ghi riêng biệt. "
        + "Reader này cho phép suy luận schema từ tất cả các sheet cần thiết "
        + "hoặc cung cấp một schema rõ ràng để diễn giải các giá trị."
        + "Xem phần Sử dụng Controller Service để biết thêm tài liệu. "
        + "Reader này hiện chỉ có khả năng xử lý các tài liệu Excel .xlsx "
        + "(định dạng XSSF 2007 OOXML) và không hỗ trợ các tài liệu .xls cũ (định dạng HSSF '97(-2007)).")
public class ExcelReader extends SchemaRegistryService implements RecordReaderFactory {

    public enum ProtectionType implements DescribedValue {
        UNPROTECTED("Không bảo vệ", "Một bảng tính Excel không được bảo vệ bằng mật khẩu"),
        PASSWORD("Bảo vệ bằng mật khẩu", "Một bảng tính Excel được bảo vệ bằng mật khẩu");

        ProtectionType(String displayName, String description) {
            this.displayName = displayName;
            this.description = description;
        }

        private final String displayName;
        private final String description;

        @Override
        public String getValue() {
            return name();
        }

        @Override
        public String getDisplayName() {
            return displayName;
        }

        @Override
        public String getDescription() {
            return description;
        }
    }

    public static final PropertyDescriptor REQUIRED_SHEETS = new PropertyDescriptor
            .Builder().name("Required Sheets")
            .displayName("Các Sheet Cần Thiết")
            .description("Danh sách các tên sheet trong tài liệu Excel, cách nhau bằng dấu phẩy, từ đó các dòng sẽ được trích xuất. Nếu thuộc tính này" +
                    " để trống thì tất cả các dòng từ tất cả các sheet sẽ được trích xuất. Danh sách tên phân biệt chữ hoa chữ thường. Bất kỳ sheet nào không" +
                    " được chỉ định trong giá trị này sẽ bị bỏ qua. Một ngoại lệ sẽ được ném ra nếu sheet được chỉ định không tìm thấy.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor STARTING_ROW = new PropertyDescriptor
            .Builder().name("Starting Row")
            .displayName("Dòng Bắt đầu")
            .description("Số thứ tự của dòng đầu tiên để bắt đầu xử lý (tính từ 1)." +
                    " Sử dụng thuộc tính này để bỏ qua các dòng dữ liệu ở đầu worksheet mà không thuộc tập dữ liệu." +
                    " Khi sử dụng chiến lược '" + ExcelHeaderSchemaStrategy.USE_STARTING_ROW.getValue() + "' thì dòng này nên là dòng header của cột.")
            .required(true)
            .defaultValue("1")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROTECTION_TYPE = new PropertyDescriptor
            .Builder().name("Protection Type")
            .displayName("Loại Bảo vệ")
            .description("Xác định liệu bảng tính Excel có được bảo vệ bằng mật khẩu hay không.")
            .required(true)
            .allowableValues(ProtectionType.class)
            .defaultValue(ProtectionType.UNPROTECTED.getValue())
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor
            .Builder().name("Password")
            .displayName("Mật khẩu")
            .description("Mật khẩu của bảng tính Excel được bảo vệ")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(PROTECTION_TYPE, ProtectionType.PASSWORD)
            .build();

    private volatile ConfigurationContext configurationContext;
    private volatile String dateFormat;
    private volatile String timeFormat;
    private volatile String timestampFormat;

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        this.configurationContext = context;
        this.dateFormat = context.getProperty(DateTimeUtils.DATE_FORMAT).getValue();
        this.timeFormat = context.getProperty(DateTimeUtils.TIME_FORMAT).getValue();
        this.timestampFormat = context.getProperty(DateTimeUtils.TIMESTAMP_FORMAT).getValue();
    }

    @Override
    public RecordReader createRecordReader(final Map<String, String> variables, final InputStream in, final long inputLength, final ComponentLog logger)
            throws MalformedRecordException, IOException, SchemaNotFoundException {
        // Use Mark/Reset of a BufferedInputStream in case we read from the Input Stream for the header.
        in.mark(1024 * 1024);
        final RecordSchema schema = getSchema(variables, new NonCloseableInputStream(in), null);
        in.reset();

        final List<String> requiredSheets = getRequiredSheets(variables);
        final int firstRow = getStartingRow(variables);
        final String password = configurationContext.getProperty(PASSWORD).getValue();
        final ExcelRecordReaderConfiguration configuration = new ExcelRecordReaderConfiguration.Builder()
                .withDateFormat(dateFormat)
                .withRequiredSheets(requiredSheets)
                .withFirstRow(firstRow)
                .withSchema(schema)
                .withTimeFormat(timeFormat)
                .withTimestampFormat(timestampFormat)
                .withPassword(password)
                .build();

        return new ExcelRecordReader(configuration, in, logger);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(STARTING_ROW);
        properties.add(REQUIRED_SHEETS);
        properties.add(PROTECTION_TYPE);
        properties.add(PASSWORD);
        properties.add(DateTimeUtils.DATE_FORMAT);
        properties.add(DateTimeUtils.TIME_FORMAT);
        properties.add(DateTimeUtils.TIMESTAMP_FORMAT);

        return properties;
    }

    @Override
    protected SchemaAccessStrategy getSchemaAccessStrategy(final String allowableValue, final SchemaRegistry schemaRegistry, final PropertyContext context) {
        if (allowableValue.equalsIgnoreCase(ExcelHeaderSchemaStrategy.USE_STARTING_ROW.getValue())) {
            return new ExcelHeaderSchemaStrategy(context, getLogger(), new TimeValueInference(dateFormat, timeFormat, timestampFormat));
        } else if (SchemaInferenceUtil.INFER_SCHEMA.getValue().equals(allowableValue)) {
            final RecordSourceFactory<Row> sourceFactory = (variables, in) -> new ExcelRecordSource(in, context, variables, getLogger());
            final SchemaInferenceEngine<Row> inference = new ExcelSchemaInference(new TimeValueInference(dateFormat, timeFormat, timestampFormat));
            return new InferSchemaAccessStrategy<>(sourceFactory, inference, getLogger());
        }

        return super.getSchemaAccessStrategy(allowableValue, schemaRegistry, context);
    }

    @Override
    protected List<AllowableValue> getSchemaAccessStrategyValues() {
        final List<AllowableValue> allowableValues = new ArrayList<>(super.getSchemaAccessStrategyValues());
        allowableValues.add(ExcelHeaderSchemaStrategy.USE_STARTING_ROW);
        allowableValues.add(SchemaInferenceUtil.INFER_SCHEMA);
        return allowableValues;
    }

    @Override
    protected AllowableValue getDefaultSchemaAccessStrategy() {
        return ExcelHeaderSchemaStrategy.USE_STARTING_ROW;
    }

    private int getStartingRow(final Map<String, String> variables) {
        int rawStartingRow = configurationContext.getProperty(STARTING_ROW).evaluateAttributeExpressions(variables).asInteger();
        String schemaAccessStrategy = configurationContext.getProperty(SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY).getValue();

        if (ExcelHeaderSchemaStrategy.USE_STARTING_ROW.getValue().equals(schemaAccessStrategy)) {
            rawStartingRow++;
        }
        return getZeroBasedIndex(rawStartingRow);
    }

    static int getZeroBasedIndex(int rawStartingRow) {
        return rawStartingRow > 0 ? rawStartingRow - 1 : 0;
    }

    private List<String> getRequiredSheets(final Map<String, String> attributes) {
        final String requiredSheetsDelimited = configurationContext.getProperty(REQUIRED_SHEETS).evaluateAttributeExpressions(attributes).getValue();
        return getRequiredSheets(requiredSheetsDelimited);
    }

    static List<String> getRequiredSheets(String requiredSheetsDelimited) {
        if (requiredSheetsDelimited != null) {
            String[] delimitedSheets = StringUtils.split(requiredSheetsDelimited, ",");
            if (delimitedSheets != null) {
                return Arrays.asList(delimitedSheets);
            }
        }

        return Collections.emptyList();
    }
}
