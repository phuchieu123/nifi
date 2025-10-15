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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.json.schema.JsonSchema;
import org.apache.nifi.schema.access.JsonSchemaRegistryComponent;
import org.apache.nifi.json.schema.SchemaVersion;
import org.apache.nifi.schemaregistry.services.JsonSchemaRegistry;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"JSON", "schema", "validation"})
@WritesAttributes({
        @WritesAttribute(attribute = ValidateJson.ERROR_ATTRIBUTE_KEY, description = "Nếu flow file được chuyển đến mối quan hệ không hợp lệ "
                + ", thuộc tính này sẽ chứa thông báo lỗi do xác thực thất bại.")
})
@CapabilityDescription("Xác thực nội dung của FlowFiles dựa trên một Lược đồ JSON có thể cấu hình. Xem json-schema.org để biết các tiêu chuẩn đặc tả. " +
        "Bộ xử lý này không hỗ trợ đầu vào chứa nhiều đối tượng JSON, chẳng hạn như JSON được phân tách bằng dòng mới. Nếu FlowFile đầu vào chứa " +
        "JSON được phân tách bằng dòng mới, chỉ dòng đầu tiên sẽ được xác thực."
)
@SystemResourceConsideration(resource = SystemResource.MEMORY, description = "Việc xác thực JSON yêu cầu đọc nội dung FlowFile vào bộ nhớ")
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.REFERENCE_REMOTE_RESOURCES,
                        explanation = "Cấu hình lược đồ có thể tham chiếu đến các tài nguyên qua HTTP"
                )
        }
)
public class ValidateJson extends AbstractProcessor {
    public enum JsonSchemaStrategy implements DescribedValue {
        SCHEMA_NAME_PROPERTY("Thuộc tính " + SCHEMA_NAME_PROPERTY_NAME,
                "Tên của Lược đồ sẽ được sử dụng được chỉ định bởi Thuộc tính '" + SCHEMA_NAME_PROPERTY_NAME +
                        "'. Giá trị của thuộc tính này được sử dụng để tra cứu Lược đồ trong Dịch vụ JSON Schema Registry đã được cấu hình."),
        SCHEMA_CONTENT_PROPERTY("Thuộc tính " + SCHEMA_CONTENT_PROPERTY_NAME,
                "Một URL hoặc đường dẫn tệp đến lược đồ JSON hoặc chính lược đồ JSON thực tế được chỉ định bởi Thuộc tính '" + SCHEMA_CONTENT_PROPERTY_NAME + "'. " +
                        "Bất kể lược đồ JSON được chỉ định như thế nào, nó phải là một lược đồ JSON hợp lệ");

        JsonSchemaStrategy(String displayName, String description) {
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

    protected static final String ERROR_ATTRIBUTE_KEY = "json.validation.errors";
    private static final String SCHEMA_NAME_PROPERTY_NAME = "Tên Lược đồ";
    private static final String SCHEMA_CONTENT_PROPERTY_NAME = "Lược đồ JSON";

    public static final PropertyDescriptor SCHEMA_ACCESS_STRATEGY = new PropertyDescriptor.Builder()
            .name("Schema Access Strategy")
            .displayName("Chiến lược Truy cập Lược đồ")
            .description("Chỉ định cách để lấy được lược đồ sẽ được sử dụng để diễn giải dữ liệu.")
            .allowableValues(JsonSchemaStrategy.class)
            .defaultValue(JsonSchemaStrategy.SCHEMA_CONTENT_PROPERTY.getValue())
            .required(true)
            .build();

    public static final PropertyDescriptor SCHEMA_NAME = new PropertyDescriptor.Builder()
            .name(SCHEMA_NAME_PROPERTY_NAME)
            .displayName(SCHEMA_NAME_PROPERTY_NAME)
            .description("Chỉ định tên của lược đồ để tra cứu trong thuộc tính Schema Registry")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("${schema.name}")
            .dependsOn(SCHEMA_ACCESS_STRATEGY, JsonSchemaStrategy.SCHEMA_NAME_PROPERTY)
            .build();

    public static final PropertyDescriptor SCHEMA_REGISTRY = new PropertyDescriptor.Builder()
            .name("JSON Schema Registry")
            .displayName("JSON Schema Registry")
            .description("Chỉ định Controller Service sẽ được sử dụng cho JSON Schema Registry")
            .identifiesControllerService(JsonSchemaRegistry.class)
            .required(true)
            .dependsOn(SCHEMA_ACCESS_STRATEGY, JsonSchemaStrategy.SCHEMA_NAME_PROPERTY)
            .build();

    public static final PropertyDescriptor SCHEMA_CONTENT = new PropertyDescriptor.Builder()
            .name(SCHEMA_CONTENT_PROPERTY_NAME)
            .displayName(SCHEMA_CONTENT_PROPERTY_NAME)
            .description("Một URL hoặc đường dẫn tệp đến lược đồ JSON hoặc nội dung lược đồ JSON thực tế")
            .required(true)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE, ResourceType.URL, ResourceType.TEXT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(SCHEMA_ACCESS_STRATEGY, JsonSchemaStrategy.SCHEMA_CONTENT_PROPERTY)
            .build();

    public static final PropertyDescriptor SCHEMA_VERSION = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(JsonSchemaRegistryComponent.SCHEMA_VERSION)
            .dependsOn(SCHEMA_ACCESS_STRATEGY, JsonSchemaStrategy.SCHEMA_CONTENT_PROPERTY)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = Arrays.asList(
            SCHEMA_ACCESS_STRATEGY,
            SCHEMA_NAME,
            SCHEMA_REGISTRY,
            SCHEMA_CONTENT,
            SCHEMA_VERSION
    );

    public static final Relationship REL_VALID = new Relationship.Builder()
            .name("valid")
            .description("Các FlowFiles được xác thực thành công dựa trên lược đồ sẽ được chuyển đến mối quan hệ này")
            .build();

    public static final Relationship REL_INVALID = new Relationship.Builder()
            .name("invalid")
            .description("Các FlowFiles không hợp lệ theo lược đồ đã chỉ định sẽ được chuyển đến mối quan hệ này")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Các FlowFiles không thể đọc được dưới dạng JSON sẽ được chuyển đến mối quan hệ này")
            .build();
    private static final Set<Relationship> RELATIONSHIPS = new HashSet<>(Arrays.asList(
            REL_VALID,
            REL_INVALID,
            REL_FAILURE
    ));

    private static final ObjectMapper MAPPER = new ObjectMapper().configure(JsonParser.Feature.ALLOW_COMMENTS, true);

    private final ConcurrentMap<SchemaVersion, JsonSchemaFactory> schemaFactories =  Arrays.stream(SchemaVersion.values())
            .collect(
                    Collectors.toConcurrentMap(
                            Function.identity(),
                            schemaDraftVersion -> JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.fromId(schemaDraftVersion.getUri()).get())
                    )
            );
    private volatile com.networknt.schema.JsonSchema schema;
    private volatile JsonSchemaRegistry jsonSchemaRegistry;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Collection<ValidationResult> validationResults = new ArrayList<>();
        final String schemaAccessStrategy = getSchemaAccessStrategy(validationContext);

        if (isNameStrategy(validationContext) && !validationContext.getProperty(SCHEMA_REGISTRY).isSet()) {
            validationResults.add(new ValidationResult.Builder()
                    .subject(SCHEMA_REGISTRY.getDisplayName())
                    .explanation(getPropertyValidateMessage(schemaAccessStrategy, SCHEMA_REGISTRY))
                    .valid(false)
                    .build());
        } else if (isContentStrategy(validationContext) && !validationContext.getProperty(SCHEMA_CONTENT).isSet()) {
            validationResults.add(new ValidationResult.Builder()
                    .subject(SCHEMA_CONTENT.getDisplayName())
                    .explanation(getPropertyValidateMessage(schemaAccessStrategy, SCHEMA_CONTENT))
                    .valid(false)
                    .build());
        }

        return validationResults;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        if (isNameStrategy(context)) {
            jsonSchemaRegistry = context.getProperty(SCHEMA_REGISTRY).asControllerService(JsonSchemaRegistry.class);
        } else if (isContentStrategy(context)) {
            try (final InputStream inputStream = context.getProperty(SCHEMA_CONTENT).asResource().read()) {
                final SchemaVersion schemaVersion = SchemaVersion.valueOf(context.getProperty(SCHEMA_VERSION).getValue());
                final JsonSchemaFactory factory = schemaFactories.get(schemaVersion);
                schema = factory.getSchema(inputStream);
            }
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        if (isNameStrategy(context)) {
            try {
                final String schemaName = context.getProperty(SCHEMA_NAME).evaluateAttributeExpressions(flowFile).getValue();
                final JsonSchema jsonSchema = jsonSchemaRegistry.retrieveSchema(schemaName);
                final JsonSchemaFactory factory = schemaFactories.get(jsonSchema.getSchemaVersion());
                schema = factory.getSchema(jsonSchema.getSchemaText());
            } catch (Exception e) {
                getLogger().error("Could not retrieve JSON schema for {}", flowFile, e);
                session.getProvenanceReporter().route(flowFile, REL_FAILURE);
                session.transfer(flowFile, REL_FAILURE);
                return;
            }
        }

        try (final InputStream in = session.read(flowFile)) {
            final JsonNode node = MAPPER.readTree(in);
            final Set<ValidationMessage> errors = schema.validate(node);

            if (errors.isEmpty()) {
                getLogger().debug("JSON {} valid", flowFile);
                session.getProvenanceReporter().route(flowFile, REL_VALID);
                session.transfer(flowFile, REL_VALID);
            } else {
                final String validationMessages = errors.toString();
                flowFile = session.putAttribute(flowFile, ERROR_ATTRIBUTE_KEY, validationMessages);
                getLogger().warn("JSON {} invalid: Validation Errors {}", flowFile, validationMessages);
                session.getProvenanceReporter().route(flowFile, REL_INVALID);
                session.transfer(flowFile, REL_INVALID);
            }
        } catch (final Exception e) {
            getLogger().error("JSON processing failed {}", flowFile, e);
            session.getProvenanceReporter().route(flowFile, REL_FAILURE);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private String getPropertyValidateMessage(String schemaAccessStrategy, PropertyDescriptor property) {
        return "The '" + schemaAccessStrategy + "' Schema Access Strategy requires that the " + property.getDisplayName() + " property be set.";
    }

    private boolean isNameStrategy(PropertyContext context) {
        final String schemaAccessStrategy = getSchemaAccessStrategy(context);
        return JsonSchemaStrategy.SCHEMA_NAME_PROPERTY.getValue().equals(schemaAccessStrategy);
    }

    private String getSchemaAccessStrategy(PropertyContext context) {
        return context.getProperty(SCHEMA_ACCESS_STRATEGY).getValue();
    }

    private boolean isContentStrategy(PropertyContext context) {
        final String schemaAccessStrategy = getSchemaAccessStrategy(context);
        return JsonSchemaStrategy.SCHEMA_CONTENT_PROPERTY.getValue().equals(schemaAccessStrategy);
    }
}