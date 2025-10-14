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
package org.apache.nifi.cef;

import com.fluenda.parcefone.parser.CEFParser;
import org.apache.bval.jsr.ApacheValidationProvider;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaAccessStrategy;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schema.inference.SchemaInferenceUtil;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SchemaRegistryService;
import org.apache.nifi.serialization.record.RecordSchema;

import javax.validation.Validation;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY;
import static org.apache.nifi.schema.inference.SchemaInferenceUtil.SCHEMA_CACHE;

@Tags({"cef", "record", "reader", "parser"})
@CapabilityDescription("Phân tích các sự kiện CEF (Common Event Format), trả về mỗi dòng dưới dạng một bản ghi. "
    + "Reader này cho phép suy luận schema dựa trên sự kiện đầu tiên trong FlowFile hoặc cung cấp một schema rõ ràng để diễn giải các giá trị.")
public final class CEFReader extends SchemaRegistryService implements RecordReaderFactory {

    static final AllowableValue HEADERS_ONLY = new AllowableValue("headers-only", "Chỉ Header", "Chỉ bao gồm các trường header CEF vào schema được suy luận.");
    static final AllowableValue HEADERS_AND_EXTENSIONS = new AllowableValue("headers-and-extensions", "Header và Extensions",
            "Bao gồm các trường header và extension của CEF vào schema, nhưng không bao gồm các extension tùy chỉnh.");
    static final AllowableValue CUSTOM_EXTENSIONS_AS_STRINGS = new AllowableValue("custom-extensions-as-string", "Với các extension tùy chỉnh dưới dạng chuỗi",
            "Bao gồm tất cả các trường vào schema được suy luận, bao gồm các trường extension tùy chỉnh dưới dạng giá trị chuỗi.");
    static final AllowableValue CUSTOM_EXTENSIONS_INFERRED = new AllowableValue("custom-extensions-inferred", "Với các extension tùy chỉnh được suy luận",
            "Bao gồm tất cả các trường vào schema được suy luận, bao gồm các trường extension tùy chỉnh với kiểu dữ liệu suy luận. " +
            "Việc suy luận dựa trên giá trị trong FlowFile. Trong một số trường hợp, điều này có thể dẫn đến hành vi không thỏa đáng. " +
            "Trong những trường hợp này, nên sử dụng chiến lược suy luận \"" + CUSTOM_EXTENSIONS_AS_STRINGS.getDisplayName() + "\" hoặc schema định sẵn.");

    static final PropertyDescriptor INFERENCE_STRATEGY = new PropertyDescriptor.Builder()
            .name("inference-strategy")
            .displayName("Chiến lược Suy luận")
            .description("Xác định các trường nên được bao gồm trong schema và cách các trường được diễn giải.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .allowableValues(HEADERS_ONLY, HEADERS_AND_EXTENSIONS, CUSTOM_EXTENSIONS_AS_STRINGS, CUSTOM_EXTENSIONS_INFERRED)
            .defaultValue(CUSTOM_EXTENSIONS_INFERRED.getValue())
            .dependsOn(SCHEMA_ACCESS_STRATEGY, SchemaInferenceUtil.INFER_SCHEMA)
            .build();

    static final PropertyDescriptor RAW_FIELD = new PropertyDescriptor.Builder()
            .name("raw-message-field")
            .displayName("Trường Thông điệp Thô")
            .description("Nếu được đặt, thông điệp thô sẽ được thêm vào bản ghi sử dụng giá trị thuộc tính làm tên trường. Điều này không giống với trường extension \"rawEvent\"!")
            .addValidator(new ValidateRawField())
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor INVALID_FIELD = new PropertyDescriptor.Builder()
            .name("invalid-message-field")
            .displayName("Trường Không hợp lệ")
            .description("Sử dụng khi một dòng trong FlowFile không thể được phân tích bởi trình phân tích CEF. " +
                    "Nếu được đặt, thay vì thất bại khi xử lý FlowFile, một bản ghi sẽ được thêm với một trường. " +
                    "Bản ghi này chứa một trường với tên được chỉ định bởi thuộc tính và thông điệp thô làm giá trị.")
            .addValidator(new ValidateRawField())
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor DATETIME_REPRESENTATION = new PropertyDescriptor.Builder()
            .name("datetime-representation")
            .displayName("Định dạng Ngày-Giờ")
            .description("Đại diện Locale theo IETF BCP 47 được sử dụng khi phân tích các trường ngày tháng với tên tháng dài hoặc ngắn (ví dụ: may <en-US> so với mai <fr-FR>). Giá trị mặc định thường an toàn. Chỉ thay đổi nếu gặp vấn đề khi phân tích các thông điệp CEF.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(new ValidateLocale())
            .defaultValue("en-US")
            .build();

    static final PropertyDescriptor ACCEPT_EMPTY_EXTENSIONS = new PropertyDescriptor.Builder()
            .name("accept-empty-extensions")
            .displayName("Chấp nhận extensions rỗng")
            .description("Nếu được đặt thành true, các extensions rỗng sẽ được chấp nhận và sẽ được liên kết với giá trị null.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .required(true)
            .defaultValue("false")
            .allowableValues("true", "false")
            .build();

    private final javax.validation.Validator validator = Validation.byProvider(ApacheValidationProvider.class).configure().buildValidatorFactory().getValidator();
    private final CEFParser parser = new CEFParser(validator);

    private volatile String rawMessageField;
    private volatile String invalidField;
    private volatile Locale parcefoneLocale;
    private volatile boolean includeCustomExtensions;
    private volatile boolean acceptEmptyExtensions;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(RAW_FIELD);
        properties.add(INVALID_FIELD);
        properties.add(DATETIME_REPRESENTATION);
        properties.add(INFERENCE_STRATEGY);

        properties.add(new PropertyDescriptor.Builder()
                .fromPropertyDescriptor(SCHEMA_CACHE)
                .dependsOn(SCHEMA_ACCESS_STRATEGY, SchemaInferenceUtil.INFER_SCHEMA)
                .build());

        properties.add(ACCEPT_EMPTY_EXTENSIONS);
        return properties;
    }

    @Override
    protected List<AllowableValue> getSchemaAccessStrategyValues() {
        final List<AllowableValue> allowableValues = new ArrayList<>(super.getSchemaAccessStrategyValues());
        allowableValues.add(SchemaInferenceUtil.INFER_SCHEMA);
        return allowableValues;
    }

    @Override
    protected AllowableValue getDefaultSchemaAccessStrategy() {
        return SchemaInferenceUtil.INFER_SCHEMA;
    }

    @Override
    protected SchemaAccessStrategy getSchemaAccessStrategy(final String strategy, final SchemaRegistry schemaRegistry, final PropertyContext context) {
        if (strategy.equals(SchemaInferenceUtil.INFER_SCHEMA.getValue())) {
            final String inferenceStrategy = context.getProperty(INFERENCE_STRATEGY).getValue();
            final CEFSchemaInferenceBuilder builder = new CEFSchemaInferenceBuilder();

            if (inferenceStrategy.equals(HEADERS_AND_EXTENSIONS.getValue())) {
                builder.withExtensions();
            } else if (inferenceStrategy.equals(CUSTOM_EXTENSIONS_AS_STRINGS.getValue())) {
                builder.withCustomExtensions(CEFCustomExtensionTypeResolver.STRING_RESOLVER);
            } else if (inferenceStrategy.equals(CUSTOM_EXTENSIONS_INFERRED.getValue())) {
                builder.withCustomExtensions(CEFCustomExtensionTypeResolver.SIMPLE_RESOLVER);
            }

            if (rawMessageField != null) {
                builder.withRawMessage(rawMessageField);
            }

            if (invalidField != null) {
                builder.withInvalidField(invalidField);
            }

            final boolean failFast = invalidField == null || invalidField.isEmpty();
            final CEFSchemaInference inference = builder.build();
            return SchemaInferenceUtil.getSchemaAccessStrategy(
                strategy,
                context,
                getLogger(),
                (variables, in) -> new CEFRecordSource(in, parser, parcefoneLocale, acceptEmptyExtensions, failFast),
                () -> inference,
                () -> super.getSchemaAccessStrategy(strategy, schemaRegistry, context));
        }

        return super.getSchemaAccessStrategy(strategy, schemaRegistry, context);
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        rawMessageField = context.getProperty(RAW_FIELD).evaluateAttributeExpressions().getValue();
        invalidField = context.getProperty(INVALID_FIELD).evaluateAttributeExpressions().getValue();
        parcefoneLocale = Locale.forLanguageTag(context.getProperty(DATETIME_REPRESENTATION).evaluateAttributeExpressions().getValue());

        final String inferenceStrategy = context.getProperty(INFERENCE_STRATEGY).getValue();
        final boolean inferenceNeedsCustomExtensions = !inferenceStrategy.equals(HEADERS_ONLY.getValue()) && !inferenceStrategy.equals(HEADERS_AND_EXTENSIONS.getValue());
        final boolean isInferSchema =  context.getProperty(SCHEMA_ACCESS_STRATEGY).getValue().equals(SchemaInferenceUtil.INFER_SCHEMA.getValue());

        includeCustomExtensions = !isInferSchema || inferenceNeedsCustomExtensions;
        acceptEmptyExtensions = context.getProperty(ACCEPT_EMPTY_EXTENSIONS).asBoolean();
    }

    @Override
    public RecordReader createRecordReader(
        final Map<String, String> variables, final InputStream in, final long inputLength, final ComponentLog logger
    ) throws MalformedRecordException, IOException, SchemaNotFoundException {
        final RecordSchema schema = getSchema(variables, in, null);
        return new CEFRecordReader(in, schema, parser, logger, parcefoneLocale, rawMessageField, invalidField, includeCustomExtensions, acceptEmptyExtensions);
    }

    private static class ValidateRawField implements Validator {
        private final Set<String> headerFields = CEFSchemaUtil.getHeaderFields().stream().map(r -> r.getFieldName()).collect(Collectors.toSet());
        private final Set<String> extensionFields = CEFSchemaUtil.getExtensionTypeMapping().keySet().stream().flatMap(fields -> fields.stream()).collect(Collectors.toSet());

        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            if (headerFields.contains(input)) {
                return new ValidationResult.Builder().subject(subject).input(input).valid(false)
                        .explanation(input + " is one of the CEF header fields.").build();
            }

            if (extensionFields.contains(input)) {
                return new ValidationResult.Builder().subject(subject).input(input).valid(false)
                        .explanation(input + " is one of the CEF extension fields.").build();
            }

            // Field names are not part of the specified CEF field names are accepted, just like null or empty value
            return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
        }
    }
}
