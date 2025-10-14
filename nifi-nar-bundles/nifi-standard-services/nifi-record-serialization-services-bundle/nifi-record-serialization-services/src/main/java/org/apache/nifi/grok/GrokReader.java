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

package org.apache.nifi.grok;

import io.krakens.grok.api.Grok;
import io.krakens.grok.api.GrokCompiler;
import io.krakens.grok.api.GrokUtils;
import io.krakens.grok.api.exception.GrokException;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceReference;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
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
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

@Tags({"grok", "logs", "logfiles", "parse", "unstructured", "text", "record", "reader", "regex", "pattern", "logstash"})
@CapabilityDescription("Cung cấp cơ chế đọc dữ liệu văn bản không cấu trúc, chẳng hạn như các file log, "
        + "và cấu trúc hóa dữ liệu để có thể xử lý. Dịch vụ được cấu hình sử dụng các mẫu Grok. "
        + "Dịch vụ đọc từ luồng dữ liệu và tách mỗi thông điệp mà nó tìm thấy thành một Record riêng biệt, "
        + "mỗi Record chứa các trường đã được cấu hình. "
        + "Nếu một dòng trong đầu vào không khớp với mẫu thông điệp mong đợi, dòng văn bản đó sẽ được xem là "
        + "một phần của thông điệp trước đó hoặc bị bỏ qua, tùy thuộc vào cấu hình, ngoại trừ các stack trace. "
        + "Một stack trace được tìm thấy ở cuối một thông điệp log sẽ được xem là một phần của thông điệp trước đó "
        + "nhưng được thêm vào trường 'stackTrace' của Record. Nếu Record không có stack trace, nó sẽ có giá trị NULL "
        + "cho trường stackTrace (giả sử schema thực sự bao gồm trường stackTrace kiểu String). "
        + "Giả sử schema bao gồm trường '_raw' kiểu String, thông điệp gốc sẽ được đưa vào Record.")
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.REFERENCE_REMOTE_RESOURCES,
                        explanation = "Các Patterns và Biểu thức có thể tham chiếu tài nguyên qua HTTP"
                )
        }
)
public class GrokReader extends SchemaRegistryService implements RecordReaderFactory {
    private volatile List<Grok> groks;
    private volatile NoMatchStrategy noMatchStrategy;
    private volatile RecordSchema recordSchema;
    private volatile RecordSchema recordSchemaFromGrok;

    static final String DEFAULT_PATTERN_NAME = "/default-grok-patterns.txt";

    static final AllowableValue APPEND_TO_PREVIOUS_MESSAGE = new AllowableValue(
        "append-to-previous-message", 
        "Thêm vào Thông điệp Trước", 
        "Dòng văn bản không khớp với Biểu thức Grok sẽ được thêm vào trường cuối cùng của thông điệp trước đó."
    );
    static final AllowableValue SKIP_LINE = new AllowableValue(
        "skip-line", 
        "Bỏ qua Dòng", 
        "Dòng văn bản không khớp với Biểu thức Grok sẽ bị bỏ qua."
    );
    static final AllowableValue RAW_LINE = new AllowableValue(
        "raw-line", 
        "Dòng Gốc", 
        "Dòng văn bản không khớp với Biểu thức Grok sẽ chỉ được thêm vào trường _raw."
    );

    static final AllowableValue STRING_FIELDS_FROM_GROK_EXPRESSION = new AllowableValue(
        "string-fields-from-grok-expression", 
        "Sử dụng Trường String từ Biểu thức Grok", 
        "Schema sẽ được suy ra dựa trên tên các trường có trong tất cả các Biểu thức Grok được cấu hình. "
        + "Tất cả các trường trong schema sẽ có kiểu String và được đánh dấu là nullable. "
        + "Schema cũng sẽ bao gồm trường `stackTrace` và trường `_raw` chứa chuỗi dòng đầu vào."
    );

    static final PropertyDescriptor GROK_PATTERNS = new PropertyDescriptor.Builder()
        .name("Grok Pattern File")
        .displayName("Các Mẫu Grok")
        .description("Các mẫu Grok để sử dụng phân tích log. Nếu không chỉ định, một file Pattern mặc định sẽ được sử dụng. "
            + "Nếu chỉ định, tất cả các pattern được cung cấp sẽ ghi đè pattern mặc định. Xem chi tiết bổ sung của Controller Service để biết danh sách các pattern định nghĩa sẵn.")
        .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE, ResourceType.URL, ResourceType.TEXT)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .required(false)
        .build();

    static final PropertyDescriptor GROK_EXPRESSION = new PropertyDescriptor.Builder()
        .name("Grok Expression")
        .displayName("Biểu thức Grok")
        .description("Xác định định dạng của một dòng log theo định dạng Grok. Điều này cho phép Record Reader hiểu cách phân tích từng dòng log. "
            + "Thuộc tính hỗ trợ một hoặc nhiều biểu thức Grok. Reader sẽ cố gắng phân tích các dòng đầu vào theo thứ tự biểu thức được cấu hình. "
            + "Nếu một dòng trong file log không khớp với bất kỳ biểu thức nào, dòng đó sẽ được coi là thuộc về thông điệp log trước đó. "
            + "Nếu các Grok pattern khác được tham chiếu bởi biểu thức này, chúng cần được cung cấp trong thuộc tính Grok Pattern File.")
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.TEXT, ResourceType.URL, ResourceType.FILE)
        .required(true)
        .build();

    static final PropertyDescriptor NO_MATCH_BEHAVIOR = new PropertyDescriptor.Builder()
        .name("no-match-behavior")
        .displayName("Hành vi khi Không khớp")
        .description("Nếu một dòng văn bản được gặp mà không khớp với Biểu thức Grok đã cho, và không phải là một stack trace, "
            + "thuộc tính này xác định cách xử lý dòng văn bản đó.")
        .allowableValues(APPEND_TO_PREVIOUS_MESSAGE, SKIP_LINE, RAW_LINE)
        .defaultValue(APPEND_TO_PREVIOUS_MESSAGE.getValue())
        .required(true)
        .build();


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(GROK_PATTERNS);
        properties.add(GROK_EXPRESSION);
        properties.add(NO_MATCH_BEHAVIOR);
        return properties;
    }

    @OnEnabled
    public void preCompile(final ConfigurationContext context) throws GrokException, IOException {
        GrokCompiler grokCompiler = GrokCompiler.newInstance();

        try (final Reader defaultPatterns = getDefaultPatterns()) {
            grokCompiler.register(defaultPatterns);
        }

        if (context.getProperty(GROK_PATTERNS).isSet()) {
            try (final InputStream patterns = context.getProperty(GROK_PATTERNS).evaluateAttributeExpressions().asResource().read()) {
                grokCompiler.register(patterns);
            }
        }

        groks = readGrokExpressions(context).stream()
                .map(grokCompiler::compile)
                .collect(Collectors.toList());

        if (context.getProperty(NO_MATCH_BEHAVIOR).getValue().equalsIgnoreCase(APPEND_TO_PREVIOUS_MESSAGE.getValue())) {
            noMatchStrategy = NoMatchStrategy.APPEND;
        } else if (context.getProperty(NO_MATCH_BEHAVIOR).getValue().equalsIgnoreCase(RAW_LINE.getValue())) {
            noMatchStrategy = NoMatchStrategy.RAW;
        } else {
            noMatchStrategy = NoMatchStrategy.SKIP;
        }

        this.recordSchemaFromGrok = createRecordSchema(groks);

        final String schemaAccess = context.getProperty(getSchemaAcessStrategyDescriptor()).getValue();
        if (STRING_FIELDS_FROM_GROK_EXPRESSION.getValue().equals(schemaAccess)) {
            this.recordSchema = recordSchemaFromGrok;
        } else {
            this.recordSchema = null;
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));
        final GrokCompiler grokCompiler = GrokCompiler.newInstance();

        final String expressionSubject = GROK_EXPRESSION.getDisplayName();

        try (final Reader defaultPatterns = getDefaultPatterns()) {
            grokCompiler.register(defaultPatterns);
        } catch (final IOException e) {
            results.add(new ValidationResult.Builder()
                    .input("Default Grok Patterns")
                    .subject(expressionSubject)
                    .valid(false)
                    .explanation("Unable to load default patterns: " + e.getMessage())
                    .build());
        }

        final ResourceReference patternsReference = validationContext.getProperty(GROK_PATTERNS).evaluateAttributeExpressions().asResource();
        final GrokExpressionValidator validator = new GrokExpressionValidator(patternsReference, grokCompiler);

        try {
            final List<String> grokExpressions = readGrokExpressions(validationContext);
            final List<ValidationResult> grokExpressionResults = grokExpressions.stream()
                    .map(grokExpression -> validator.validate(expressionSubject, grokExpression, validationContext)).collect(Collectors.toList());
            results.addAll(grokExpressionResults);
        } catch (final IOException e) {
            results.add(new ValidationResult.Builder()
                    .input("Configured Grok Expressions")
                    .subject(expressionSubject)
                    .valid(false)
                    .explanation(String.format("Read Grok Expressions failed: %s", e.getMessage()))
                    .build());
        }

        return results;
    }

    private List<String> readGrokExpressions(final PropertyContext propertyContext) throws IOException {
        final ResourceReference expressionsResource = propertyContext.getProperty(GROK_EXPRESSION).asResource();
        try (
                final InputStream expressionsStream = expressionsResource.read();
                final BufferedReader expressionsReader = new BufferedReader(new InputStreamReader(expressionsStream))
        ) {
            return expressionsReader.lines().collect(Collectors.toList());
        }
    }

    static RecordSchema createRecordSchema(final List<Grok> groks) {
        final Set<RecordField> fields = new LinkedHashSet<>();

        groks.forEach(grok -> populateSchemaFieldNames(grok, grok.getOriginalGrokPattern(), fields));

        fields.add(new RecordField(GrokRecordReader.STACK_TRACE_COLUMN_NAME, RecordFieldType.STRING.getDataType(), true));
        fields.add(new RecordField(GrokRecordReader.RAW_MESSAGE_NAME, RecordFieldType.STRING.getDataType(), true));

        return new SimpleRecordSchema(new ArrayList<>(fields));
    }

    private static void populateSchemaFieldNames(final Grok grok, String grokExpression, final Collection<RecordField> fields) {
        final Set<String> namedGroups = GrokUtils.getNameGroups(GrokUtils.GROK_PATTERN.pattern());
        while (grokExpression.length() > 0) {
            final Matcher matcher = GrokUtils.GROK_PATTERN.matcher(grokExpression);
            if (matcher.find()) {
                final Map<String, String> extractedGroups = GrokUtils.namedGroups(matcher, namedGroups);
                final String subName = extractedGroups.get("subname");

                if (subName == null) {
                    final String subPatternName = extractedGroups.get("pattern");
                    if (subPatternName == null) {
                        continue;
                    }

                    final String subExpression = grok.getPatterns().get(subPatternName);
                    populateSchemaFieldNames(grok, subExpression, fields);
                } else {
                    DataType dataType = RecordFieldType.STRING.getDataType();
                    final RecordField recordField = new RecordField(subName, dataType);
                    fields.add(recordField);
                }

                if (grokExpression.length() > matcher.end() + 1) {
                    grokExpression = grokExpression.substring(matcher.end());
                } else {
                    break;
                }
            } else {
                break;
            }
        }
    }


    @Override
    protected List<AllowableValue> getSchemaAccessStrategyValues() {
        final List<AllowableValue> allowableValues = new ArrayList<>();
        allowableValues.add(STRING_FIELDS_FROM_GROK_EXPRESSION);
        allowableValues.addAll(super.getSchemaAccessStrategyValues());
        return allowableValues;
    }

    @Override
    protected AllowableValue getDefaultSchemaAccessStrategy() {
        return STRING_FIELDS_FROM_GROK_EXPRESSION;
    }

    @Override
    protected SchemaAccessStrategy getSchemaAccessStrategy(final String strategy, final SchemaRegistry schemaRegistry, final PropertyContext context) {
        if (strategy.equalsIgnoreCase(STRING_FIELDS_FROM_GROK_EXPRESSION.getValue())) {
            return createAccessStrategy();
        } else {
            return super.getSchemaAccessStrategy(strategy, schemaRegistry, context);
        }
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
        return new GrokRecordReader(in, groks, schema, recordSchemaFromGrok, noMatchStrategy);
    }

    private Reader getDefaultPatterns() throws IOException {
        final InputStream inputStream = getClass().getResourceAsStream(DEFAULT_PATTERN_NAME);
        if (inputStream == null) {
            throw new IOException(String.format("Default Patterns [%s] not found", DEFAULT_PATTERN_NAME));
        }
        return new InputStreamReader(inputStream);
    }
}