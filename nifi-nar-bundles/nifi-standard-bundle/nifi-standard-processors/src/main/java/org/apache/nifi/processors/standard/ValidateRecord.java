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

package org.apache.nifi.processors.standard;


import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.avro.AvroSchemaValidator;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schema.validation.SchemaValidationContext;
import org.apache.nifi.schema.validation.StandardSchemaValidator;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.RawRecordWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.serialization.record.validation.RecordSchemaValidator;
import org.apache.nifi.serialization.record.validation.SchemaValidationResult;
import org.apache.nifi.serialization.record.validation.ValidationError;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"record", "schema", "validate"})
@CapabilityDescription("Xác thực các Bản ghi (Records) của một FlowFile đến dựa trên một lược đồ đã cho. Tất cả các bản ghi tuân thủ lược đồ sẽ được chuyển đến mối quan hệ \"valid\" trong khi "
    + "các bản ghi không tuân thủ lược đồ sẽ được chuyển đến mối quan hệ \"invalid\". Do đó, một FlowFile đến duy nhất có thể được chia thành hai FlowFile riêng lẻ "
    + "nếu một số bản ghi hợp lệ theo lược đồ và những bản ghi khác thì không. Bất kỳ FlowFile nào được chuyển đến mối quan hệ \"invalid\" sẽ phát ra một Sự kiện Xuất xứ (Provenance Event) loại ROUTE "
    + "với trường Chi tiết (Details) được điền để giải thích tại sao các bản ghi không hợp lệ. Ngoài ra, để có thêm giải thích về lý do tại sao các bản ghi không hợp lệ, có thể bật ghi nhật ký (logging) ở cấp độ DEBUG "
    + "cho logger \"org.apache.nifi.processors.standard.ValidateRecord\".")
@WritesAttributes({
    @WritesAttribute(attribute = "mime.type", description = "Đặt thuộc tính mime.type thành Loại MIME được chỉ định bởi Record Writer"),
    @WritesAttribute(attribute = "record.count", description = "Số lượng bản ghi trong FlowFile được chuyển đến một mối quan hệ")
})
public class ValidateRecord extends AbstractProcessor {

    static final AllowableValue SCHEMA_NAME_PROPERTY = new AllowableValue("schema-name-property", "Sử dụng Thuộc tính Tên Lược đồ",
        "Lược đồ để xác thực dữ liệu được xác định bằng cách xem xét thuộc tính 'Tên Lược đồ' (Schema Name) và tra cứu lược đồ trong Schema Registry đã được cấu hình");
    static final AllowableValue SCHEMA_TEXT_PROPERTY = new AllowableValue("schema-text-property", "Sử dụng Thuộc tính Văn bản Lược đồ",
        "Lược đồ để xác thực dữ liệu được xác định bằng cách xem xét thuộc tính 'Văn bản Lược đồ' (Schema Text) và phân tích cú pháp lược đồ dưới dạng lược đồ Avro");
    static final AllowableValue READER_SCHEMA = new AllowableValue("reader-schema", "Sử dụng Lược đồ của Reader",
        "Lược đồ để xác thực dữ liệu được xác định bằng cách yêu cầu lược đồ từ Record Reader đã được cấu hình");

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
        .name("record-reader")
        .displayName("Record Reader")
        .description("Chỉ định Controller Service sẽ được sử dụng để đọc dữ liệu đến")
        .identifiesControllerService(RecordReaderFactory.class)
        .required(true)
        .build();
    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
        .name("record-writer")
        .displayName("Record Writer")
        .description("Chỉ định Controller Service sẽ được sử dụng để ghi ra các bản ghi. "
            + "Bất kể cấu hình truy cập lược đồ của Controller Service là gì, "
            + "lược đồ được sử dụng để xác thực bản ghi sẽ được dùng để ghi các kết quả hợp lệ.")
        .identifiesControllerService(RecordSetWriterFactory.class)
        .required(true)
        .build();
    static final PropertyDescriptor INVALID_RECORD_WRITER = new PropertyDescriptor.Builder()
        .name("invalid-record-writer")
        .displayName("Record Writer cho các Bản ghi không hợp lệ")
        .description("Nếu được chỉ định, Controller Service này sẽ được sử dụng để ghi ra bất kỳ bản ghi nào không hợp lệ. "
            + "Nếu không được chỉ định, writer được chỉ định bởi thuộc tính \"Record Writer\" sẽ được sử dụng với lược đồ dùng để đọc các bản ghi đầu vào. "
            + "Điều này hữu ích, ví dụ, khi Record Writer được cấu hình "
            + "không thể ghi dữ liệu không tuân thủ lược đồ của nó (như trong trường hợp của Avro) hoặc khi mong muốn giữ lại các bản ghi không hợp lệ "
            + "ở định dạng ban đầu trong khi chuyển đổi các bản ghi hợp lệ sang một định dạng khác.")
        .identifiesControllerService(RecordSetWriterFactory.class)
        .required(false)
        .build();
    static final PropertyDescriptor SCHEMA_ACCESS_STRATEGY = new PropertyDescriptor.Builder()
        .name("schema-access-strategy")
        .displayName("Chiến lược Truy cập Lược đồ")
        .description("Chỉ định cách để lấy được lược đồ sẽ được sử dụng để xác thực các bản ghi")
        .allowableValues(READER_SCHEMA, SCHEMA_NAME_PROPERTY, SCHEMA_TEXT_PROPERTY)
        .defaultValue(READER_SCHEMA.getValue())
        .required(true)
        .build();
    public static final PropertyDescriptor SCHEMA_REGISTRY = new PropertyDescriptor.Builder()
        .name("schema-registry")
        .displayName("Schema Registry")
        .description("Chỉ định Controller Service sẽ được sử dụng cho Schema Registry. Điều này chỉ cần thiết nếu Chiến lược Truy cập Lược đồ được đặt thành \"Sử dụng Thuộc tính 'Tên Lược đồ'\".")
        .identifiesControllerService(SchemaRegistry.class)
        .required(false)
        .build();
    static final PropertyDescriptor SCHEMA_NAME = new PropertyDescriptor.Builder()
        .name("schema-name")
        .displayName("Tên Lược đồ")
        .description("Chỉ định tên của lược đồ để tra cứu trong thuộc tính Schema Registry")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue("${schema.name}")
        .required(false)
        .build();
    static final PropertyDescriptor SCHEMA_TEXT = new PropertyDescriptor.Builder()
        .name("schema-text")
        .displayName("Văn bản Lược đồ")
        .description("Văn bản của một Lược đồ có định dạng Avro")
        .addValidator(new AvroSchemaValidator())
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue("${avro.schema}")
        .required(false)
        .build();
    static final PropertyDescriptor ALLOW_EXTRA_FIELDS = new PropertyDescriptor.Builder()
        .name("allow-extra-fields")
        .displayName("Cho phép các Trường Thừa")
        .description("Nếu dữ liệu đến có các trường không có trong lược đồ, thuộc tính này xác định liệu Bản ghi có hợp lệ hay không. "
            + "Nếu là true, Bản ghi vẫn hợp lệ. Nếu là false, Bản ghi sẽ không hợp lệ do có các trường thừa.")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .allowableValues("true", "false")
        .defaultValue("true")
        .required(true)
        .build();
    static final PropertyDescriptor STRICT_TYPE_CHECKING = new PropertyDescriptor.Builder()
        .name("strict-type-checking")
        .displayName("Kiểm tra Kiểu Nghiêm ngặt")
        .description("Nếu dữ liệu đến có một Bản ghi mà một trường không đúng kiểu, thuộc tính này xác định cách xử lý Bản ghi đó. "
            + "Nếu là true, Bản ghi sẽ được coi là không hợp lệ. Nếu là false, Bản ghi sẽ được coi là hợp lệ và trường đó sẽ được ép kiểu thành "
            + "kiểu đúng (nếu có thể, theo cơ chế ép kiểu được hỗ trợ bởi Record Writer). "
            + "Thuộc tính này kiểm soát cách dữ liệu được xác thực so với lược đồ xác thực.")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .allowableValues("true", "false")
        .defaultValue("true")
        .required(true)
        .build();
    static final PropertyDescriptor COERCE_TYPES = new PropertyDescriptor.Builder()
            .name("coerce-types")
            .displayName("Ép Kiểu từ Lược đồ của Reader")
            .description("Nếu được bật, bộ xử lý sẽ ép kiểu mọi trường về kiểu được chỉ định trong lược đồ của Reader. "
                + "Nếu giá trị của một trường không thể được ép về kiểu đó, trường đó sẽ bị bỏ qua (sẽ không được đọc từ dữ liệu đầu vào), "
                + "do đó sẽ không xuất hiện trong đầu ra. "
                + "Nếu không được bật, thì mọi trường sẽ xuất hiện trong đầu ra nhưng kiểu của chúng có thể khác với những gì "
                + "được chỉ định trong lược đồ. Để biết chi tiết, vui lòng xem trang Chi tiết Bổ sung trong phần Trợ giúp của bộ xử lý. "
                + "Thuộc tính này kiểm soát cách dữ liệu được đọc bởi Record Reader đã chỉ định.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();
    static final PropertyDescriptor VALIDATION_DETAILS_ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
        .name("validation-details-attribute-name")
        .displayName("Tên Thuộc tính Chi tiết Xác thực")
        .description("Nếu được chỉ định, khi có lỗi xác thực xảy ra, tên thuộc tính này sẽ được sử dụng để lưu lại chi tiết. Số lượng ký tự sẽ bị giới hạn "
            + "bởi thuộc tính 'Độ dài Tối đa của Chi tiết Xác thực'.")
        .required(false)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.ATTRIBUTE_KEY_VALIDATOR)
        .defaultValue(null)
        .build();
    static final PropertyDescriptor MAX_VALIDATION_DETAILS_LENGTH = new PropertyDescriptor.Builder()
        .name("maximum-validation-details-length")
        .displayName("Độ dài Tối đa của Chi tiết Xác thực")
        .description("Chỉ định số lượng ký tự tối đa mà giá trị chi tiết xác thực có thể có. Bất kỳ ký tự nào vượt quá giới hạn sẽ bị cắt bỏ. "
            + "Thuộc tính này chỉ được sử dụng nếu 'Tên Thuộc tính Chi tiết Xác thực' được đặt")
        .required(false)
        .defaultValue("1024")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .build();

    static final Relationship REL_VALID = new Relationship.Builder()
        .name("valid")
        .description("Các bản ghi hợp lệ theo lược đồ sẽ được chuyển đến mối quan hệ này")
        .build();
    static final Relationship REL_INVALID = new Relationship.Builder()
        .name("invalid")
        .description("Các bản ghi không hợp lệ theo lược đồ sẽ được chuyển đến mối quan hệ này")
        .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("Nếu các bản ghi không thể được đọc, xác thực hoặc ghi vì bất kỳ lý do gì, FlowFile gốc sẽ được chuyển đến mối quan hệ này")
        .build();


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(RECORD_READER);
        properties.add(RECORD_WRITER);
        properties.add(INVALID_RECORD_WRITER);
        properties.add(SCHEMA_ACCESS_STRATEGY);
        properties.add(SCHEMA_REGISTRY);
        properties.add(SCHEMA_NAME);
        properties.add(SCHEMA_TEXT);
        properties.add(ALLOW_EXTRA_FIELDS);
        properties.add(STRICT_TYPE_CHECKING);
        properties.add(COERCE_TYPES);
        properties.add(VALIDATION_DETAILS_ATTRIBUTE_NAME);
        properties.add(MAX_VALIDATION_DETAILS_LENGTH);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_VALID);
        relationships.add(REL_INVALID);
        relationships.add(REL_FAILURE);
        return relationships;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final String schemaAccessStrategy = validationContext.getProperty(SCHEMA_ACCESS_STRATEGY).getValue();
        if (schemaAccessStrategy.equals(SCHEMA_NAME_PROPERTY.getValue())) {
            if (!validationContext.getProperty(SCHEMA_REGISTRY).isSet()) {
                return Collections.singleton(new ValidationResult.Builder()
                    .subject("Schema Registry")
                    .valid(false)
                    .explanation("If the Schema Access Strategy is set to \"Use 'Schema Name' Property\", the Schema Registry property must also be set")
                    .build());
            }

            final SchemaRegistry registry = validationContext.getProperty(SCHEMA_REGISTRY).asControllerService(SchemaRegistry.class);
            if (!registry.getSuppliedSchemaFields().contains(SchemaField.SCHEMA_NAME)) {
                return Collections.singleton(new ValidationResult.Builder()
                    .subject("Schema Registry")
                    .valid(false)
                    .explanation("The configured Schema Registry does not support accessing schemas by name")
                    .build());
            }
        }

        return Collections.emptyList();
    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final RecordSetWriterFactory validRecordWriterFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        final RecordSetWriterFactory invalidRecordWriterFactory = context.getProperty(INVALID_RECORD_WRITER).isSet()
            ? context.getProperty(INVALID_RECORD_WRITER).asControllerService(RecordSetWriterFactory.class)
            : validRecordWriterFactory;
        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);

        final boolean allowExtraFields = context.getProperty(ALLOW_EXTRA_FIELDS).asBoolean();
        final boolean strictTypeChecking = context.getProperty(STRICT_TYPE_CHECKING).asBoolean();
        final boolean coerceTypes = context.getProperty(COERCE_TYPES).asBoolean();

        RecordSetWriter validWriter = null;
        RecordSetWriter invalidWriter = null;
        FlowFile validFlowFile = null;
        FlowFile invalidFlowFile = null;

        try (final InputStream in = session.read(flowFile);
            final RecordReader reader = readerFactory.createRecordReader(flowFile, in, getLogger())) {

            final RecordSchema validationSchema = getValidationSchema(context, flowFile, reader);
            final SchemaValidationContext validationContext = new SchemaValidationContext(validationSchema, allowExtraFields, strictTypeChecking);
            final RecordSchemaValidator validator = new StandardSchemaValidator(validationContext);

            int recordCount = 0;
            int validCount = 0;
            int invalidCount = 0;

            final Set<String> extraFields = new HashSet<>();
            final Set<String> missingFields = new HashSet<>();
            final Set<String> invalidFields = new HashSet<>();
            final Set<String> otherProblems = new HashSet<>();

            try {
                Record record;
                while ((record = reader.nextRecord(coerceTypes, false)) != null) {
                    final SchemaValidationResult result = validator.validate(record);
                    recordCount++;

                    RecordSetWriter writer;
                    if (result.isValid()) {
                        validCount++;
                        if (validFlowFile == null) {
                            validFlowFile = session.create(flowFile);
                        }

                        validWriter = writer = createIfNecessary(validWriter, validRecordWriterFactory, session, validFlowFile, validationSchema);

                    } else {
                        invalidCount++;
                        logValidationErrors(flowFile, recordCount, result);

                        if (invalidFlowFile == null) {
                            invalidFlowFile = session.create(flowFile);
                        }

                        invalidWriter = writer = createIfNecessary(invalidWriter, invalidRecordWriterFactory, session, invalidFlowFile, record.getSchema());

                        // Add all of the validation errors to our Set<ValidationError> but only keep up to MAX_VALIDATION_ERRORS because if
                        // we keep too many then we both use up a lot of heap and risk outputting so much information in the Provenance Event
                        // that it is too noisy to be useful.
                        for (final ValidationError validationError : result.getValidationErrors()) {
                            final Optional<String> fieldName = validationError.getFieldName();

                            switch (validationError.getType()) {
                                case EXTRA_FIELD:
                                    if (fieldName.isPresent()) {
                                        extraFields.add(fieldName.get());
                                    } else {
                                        otherProblems.add(validationError.getExplanation());
                                    }
                                    break;
                                case MISSING_FIELD:
                                    if (fieldName.isPresent()) {
                                        missingFields.add(fieldName.get());
                                    } else {
                                        otherProblems.add(validationError.getExplanation());
                                    }
                                    break;
                                case INVALID_FIELD:
                                    if (fieldName.isPresent()) {
                                        invalidFields.add(fieldName.get());
                                    } else {
                                        otherProblems.add(validationError.getExplanation());
                                    }
                                    break;
                                case OTHER:
                                    otherProblems.add(validationError.getExplanation());
                                    break;
                            }
                        }
                    }

                    if (writer instanceof RawRecordWriter) {
                        ((RawRecordWriter) writer).writeRawRecord(record);
                    } else {
                        writer.write(record);
                    }
                }

                if (validWriter != null) {
                    completeFlowFile(context, session, validFlowFile, validWriter, REL_VALID, null);
                }

                if (invalidWriter != null) {
                    // Build up a String that explains why the records were invalid, so that we can add this to the Provenance Event.
                    final StringBuilder errorBuilder = new StringBuilder();
                    errorBuilder.append("Records in this FlowFile were invalid for the following reasons: ");
                    if (!missingFields.isEmpty()) {
                        errorBuilder.append("The following ").append(missingFields.size()).append(" fields were missing: ").append(missingFields.toString());
                    }

                    if (!extraFields.isEmpty()) {
                        if (errorBuilder.length() > 0) {
                            errorBuilder.append("; ");
                        }

                        errorBuilder.append("The following ").append(extraFields.size())
                            .append(" fields were present in the Record but not in the schema: ").append(extraFields.toString());
                    }

                    if (!invalidFields.isEmpty()) {
                        if (errorBuilder.length() > 0) {
                            errorBuilder.append("; ");
                        }

                        errorBuilder.append("The following ").append(invalidFields.size())
                            .append(" fields had values whose type did not match the schema: ").append(invalidFields.toString());
                    }

                    if (!otherProblems.isEmpty()) {
                        if (errorBuilder.length() > 0) {
                            errorBuilder.append("; ");
                        }

                        errorBuilder.append("The following ").append(otherProblems.size())
                            .append(" additional problems were encountered: ").append(otherProblems.toString());
                    }

                    final String validationErrorString = errorBuilder.toString();
                    completeFlowFile(context, session, invalidFlowFile, invalidWriter, REL_INVALID, validationErrorString);
                }
            } finally {
                closeQuietly(validWriter);
                closeQuietly(invalidWriter);
            }

            session.adjustCounter("Records Validated", recordCount, false);
            session.adjustCounter("Records Found Valid", validCount, false);
            session.adjustCounter("Records Found Invalid", invalidCount, false);
        } catch (final Exception e) {
            getLogger().error("Failed to process {}", flowFile, e);
            session.transfer(flowFile, REL_FAILURE);
            if (validFlowFile != null) {
                session.remove(validFlowFile);
            }
            if (invalidFlowFile != null) {
                session.remove(invalidFlowFile);
            }
            return;
        }

        session.remove(flowFile);
    }

    private void closeQuietly(final RecordSetWriter writer) {
        if (writer != null) {
            try {
                writer.close();
            } catch (final Exception e) {
                getLogger().error("Failed to close Record Writer", e);
            }
        }
    }

    private void completeFlowFile(final ProcessContext context, final ProcessSession session, final FlowFile flowFile, final RecordSetWriter writer,
            final Relationship relationship, final String details) throws IOException {
        final WriteResult writeResult = writer.finishRecordSet();
        writer.close();

        final String validationDetailsAttributeName = context.getProperty(VALIDATION_DETAILS_ATTRIBUTE_NAME)
                .evaluateAttributeExpressions(flowFile).getValue();

        final Integer maxValidationDetailsLength = context.getProperty(MAX_VALIDATION_DETAILS_LENGTH).evaluateAttributeExpressions(flowFile).asInteger();

        final Map<String, String> attributes = new HashMap<>();
        attributes.putAll(writeResult.getAttributes());
        attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
        attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());

        if(validationDetailsAttributeName != null && details != null && !details.isEmpty()) {
            String truncatedDetails = details;

            //Truncating only when it exceeds the configured maximum
            if (truncatedDetails.length() > maxValidationDetailsLength) {
                truncatedDetails = truncatedDetails.substring(0, maxValidationDetailsLength);
            }

            attributes.put(validationDetailsAttributeName, truncatedDetails);
        }

        session.putAllAttributes(flowFile, attributes);

        session.transfer(flowFile, relationship);
        session.getProvenanceReporter().route(flowFile, relationship, details);
    }

    private RecordSetWriter createIfNecessary(final RecordSetWriter writer, final RecordSetWriterFactory factory, final ProcessSession session,
        final FlowFile flowFile, final RecordSchema outputSchema) throws SchemaNotFoundException, IOException {
        if (writer != null) {
            return writer;
        }

        final OutputStream out = session.write(flowFile);
        final RecordSetWriter created = factory.createWriter(getLogger(), outputSchema, out, flowFile);
        created.beginRecordSet();
        return created;
    }

    private void logValidationErrors(final FlowFile flowFile, final int recordCount, final SchemaValidationResult result) {
        if (getLogger().isDebugEnabled()) {
            final StringBuilder sb = new StringBuilder();
            sb.append("For ").append(flowFile).append(" Record #").append(recordCount).append(" is invalid due to:\n");
            for (final ValidationError error : result.getValidationErrors()) {
                sb.append(error).append("\n");
            }

            getLogger().debug(sb.toString());
        }
    }

    protected RecordSchema getValidationSchema(final ProcessContext context, final FlowFile flowFile, final RecordReader reader)
        throws MalformedRecordException, IOException, SchemaNotFoundException {
        final String schemaAccessStrategy = context.getProperty(SCHEMA_ACCESS_STRATEGY).getValue();
        if (schemaAccessStrategy.equals(READER_SCHEMA.getValue())) {
            return reader.getSchema();
        } else if (schemaAccessStrategy.equals(SCHEMA_NAME_PROPERTY.getValue())) {
            final SchemaRegistry schemaRegistry = context.getProperty(SCHEMA_REGISTRY).asControllerService(SchemaRegistry.class);
            final String schemaName = context.getProperty(SCHEMA_NAME).evaluateAttributeExpressions(flowFile).getValue();
            final SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder().name(schemaName).build();
            return schemaRegistry.retrieveSchema(schemaIdentifier);
        } else if (schemaAccessStrategy.equals(SCHEMA_TEXT_PROPERTY.getValue())) {
            final String schemaText = context.getProperty(SCHEMA_TEXT).evaluateAttributeExpressions(flowFile).getValue();
            final Parser parser = new Schema.Parser();
            final Schema avroSchema = parser.parse(schemaText);
            return AvroTypeUtil.createSchema(avroSchema);
        } else {
            throw new ProcessException("Invalid Schema Access Strategy: " + schemaAccessStrategy);
        }
    }
}
