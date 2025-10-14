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
package org.apache.nifi.schema.access;

import org.apache.nifi.avro.AvroSchemaValidator;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class SchemaAccessUtils {

  public static final AllowableValue SCHEMA_NAME_PROPERTY = new AllowableValue(
        "schema-name", 
        "Sử dụng thuộc tính 'Schema Name'",
        "Tên của Schema được chỉ định bởi thuộc tính 'Schema Name'. Giá trị này được dùng để tra cứu Schema trong dịch vụ Schema Registry đã cấu hình."
);

public static final AllowableValue SCHEMA_TEXT_PROPERTY = new AllowableValue(
        "schema-text-property", 
        "Sử dụng thuộc tính 'Schema Text'",
        "Nội dung của Schema được chỉ định bởi thuộc tính 'Schema Text'. Giá trị này phải là một Avro Schema hợp lệ. "
        + "Nếu sử dụng Expression Language, giá trị phải hợp lệ sau khi thay thế các biểu thức."
);

public static final AllowableValue HWX_CONTENT_ENCODED_SCHEMA = new AllowableValue(
        "hwx-content-encoded-schema", 
        "HWX Content-Encoded Schema Reference",
        "Nội dung của FlowFile chứa tham chiếu đến một schema trong Schema Registry. Tham chiếu được mã hóa bằng một byte duy nhất thể hiện 'phiên bản giao thức', "
        + "sau đó 8 byte chỉ định ID schema và 4 byte cuối chỉ định phiên bản schema, theo chuẩn serializer/deserializer của Hortonworks Schema Registry, "
        + "xem chi tiết tại https://github.com/hortonworks/registry"
);

public static final AllowableValue HWX_SCHEMA_REF_ATTRIBUTES = new AllowableValue(
        "hwx-schema-ref-attributes", 
        "Thuộc tính tham chiếu Schema HWX",
        "FlowFile chứa 3 thuộc tính dùng để tra cứu Schema từ Schema Registry đã cấu hình: 'schema.identifier', 'schema.version', và 'schema.protocol.version'"
);

public static final AllowableValue INHERIT_RECORD_SCHEMA = new AllowableValue(
        "inherit-record-schema", 
        "Kế thừa Record Schema",
        "Schema dùng để ghi các record sẽ giống với schema được cung cấp khi Record được tạo."
);

public static final AllowableValue CONFLUENT_ENCODED_SCHEMA = new AllowableValue(
        "confluent-encoded", 
        "Confluent Content-Encoded Schema Reference",
        "Nội dung FlowFile chứa tham chiếu đến schema trong Schema Registry. Tham chiếu được mã hóa bằng 'Magic Byte' theo sau là 4 byte đại diện ID schema, "
        + "theo hướng dẫn tại http://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html (Confluent Schema Registry v3.2.x)."
);

public static final AllowableValue INFER_SCHEMA = new AllowableValue(
        "infer", 
        "Suy đoán từ kết quả"
);

public static final PropertyDescriptor SCHEMA_ACCESS_STRATEGY = new PropertyDescriptor.Builder()
        .name("schema-access-strategy")
        .displayName("Chiến lược truy cập Schema")
        .description("Xác định cách lấy schema để sử dụng khi giải thích dữ liệu.")
        .allowableValues(SCHEMA_NAME_PROPERTY, SCHEMA_TEXT_PROPERTY, HWX_SCHEMA_REF_ATTRIBUTES, HWX_CONTENT_ENCODED_SCHEMA, CONFLUENT_ENCODED_SCHEMA)
        .defaultValue(SCHEMA_NAME_PROPERTY.getValue())
        .required(true)
        .build();

public static final PropertyDescriptor SCHEMA_REGISTRY = new PropertyDescriptor.Builder()
        .name("schema-registry")
        .displayName("Schema Registry")
        .description("Xác định Controller Service dùng làm Schema Registry")
        .identifiesControllerService(SchemaRegistry.class)
        .required(false)
        .dependsOn(SCHEMA_ACCESS_STRATEGY, SCHEMA_NAME_PROPERTY, HWX_SCHEMA_REF_ATTRIBUTES, HWX_CONTENT_ENCODED_SCHEMA, CONFLUENT_ENCODED_SCHEMA)
        .build();

public static final PropertyDescriptor SCHEMA_NAME = new PropertyDescriptor.Builder()
        .name("schema-name")
        .displayName("Schema Name")
        .description("Xác định tên schema để tra cứu trong Schema Registry")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue("${schema.name}")
        .dependsOn(SCHEMA_ACCESS_STRATEGY, SCHEMA_NAME_PROPERTY)
        .required(false)
        .build();

public static final PropertyDescriptor SCHEMA_BRANCH_NAME = new PropertyDescriptor.Builder()
        .name("schema-branch")
        .displayName("Chi nhánh Schema")
        .description("Xác định tên chi nhánh khi tra cứu schema trong Schema Registry. Nếu Schema Registry không hỗ trợ branching, giá trị này sẽ bị bỏ qua.")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .dependsOn(SCHEMA_ACCESS_STRATEGY, SCHEMA_NAME_PROPERTY)
        .required(false)
        .build();

public static final PropertyDescriptor SCHEMA_VERSION = new PropertyDescriptor.Builder()
        .name("schema-version")
        .displayName("Phiên bản Schema")
        .description("Xác định phiên bản schema để tra cứu trong Schema Registry. Nếu không xác định, phiên bản mới nhất sẽ được sử dụng.")
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .dependsOn(SCHEMA_ACCESS_STRATEGY, SCHEMA_NAME_PROPERTY)
        .required(false)
        .build();

public static final PropertyDescriptor SCHEMA_TEXT = new PropertyDescriptor.Builder()
        .name("schema-text")
        .displayName("Schema Text")
        .description("Nội dung Schema theo định dạng Avro")
        .addValidator(new AvroSchemaValidator())
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue("${avro.schema}")
        .dependsOn(SCHEMA_ACCESS_STRATEGY, SCHEMA_TEXT_PROPERTY)
        .required(false)
        .build();


    public static Collection<ValidationResult> validateSchemaAccessStrategy(final ValidationContext validationContext, final String schemaAccessStrategyValue,
                                                                            final List<AllowableValue> schemaAccessStrategyValues) {

        final Collection<ValidationResult> validationResults = new ArrayList<>();

        if (isSchemaRegistryRequired(schemaAccessStrategyValue)) {
            final boolean registrySet = validationContext.getProperty(SCHEMA_REGISTRY).isSet();
            if (!registrySet) {
                final String schemaAccessStrategyName = getSchemaAccessStrategyName(schemaAccessStrategyValue, schemaAccessStrategyValues);

                validationResults.add(new ValidationResult.Builder()
                        .subject("Schema Registry")
                        .explanation("The '" + schemaAccessStrategyName + "' Schema Access Strategy requires that the Schema Registry property be set.")
                        .valid(false)
                        .build());
            }
        }

        // ensure that only branch or version is specified, but not both
        if (SCHEMA_NAME_PROPERTY.getValue().equalsIgnoreCase(schemaAccessStrategyValue)) {
            final boolean branchNameSet = validationContext.getProperty(SCHEMA_BRANCH_NAME).isSet();
            final boolean versionSet = validationContext.getProperty(SCHEMA_VERSION).isSet();

            if (branchNameSet && versionSet) {
                validationResults.add(new ValidationResult.Builder()
                        .subject(SCHEMA_BRANCH_NAME.getDisplayName())
                        .explanation(SCHEMA_BRANCH_NAME.getDisplayName() + " and " + SCHEMA_VERSION.getDisplayName() + " cannot be specified together")
                        .valid(false)
                        .build());
            }
        }

        return validationResults;
    }

    private static String getSchemaAccessStrategyName(final String schemaAccessValue, final List<AllowableValue> schemaAccessStrategyValues) {
        for (final AllowableValue allowableValue : schemaAccessStrategyValues) {
            if (allowableValue.getValue().equalsIgnoreCase(schemaAccessValue)) {
                return allowableValue.getDisplayName();
            }
        }

        return null;
    }

    private static boolean isSchemaRegistryRequired(final String schemaAccessValue) {
        return HWX_CONTENT_ENCODED_SCHEMA.getValue().equalsIgnoreCase(schemaAccessValue) || SCHEMA_NAME_PROPERTY.getValue().equalsIgnoreCase(schemaAccessValue)
            || HWX_SCHEMA_REF_ATTRIBUTES.getValue().equalsIgnoreCase(schemaAccessValue) || CONFLUENT_ENCODED_SCHEMA.getValue().equalsIgnoreCase(schemaAccessValue);
    }


    public static SchemaAccessStrategy getSchemaAccessStrategy(final String allowableValue, final SchemaRegistry schemaRegistry, final PropertyContext context) {
        if (allowableValue.equalsIgnoreCase(SCHEMA_NAME_PROPERTY.getValue())) {
            final PropertyValue schemaName = context.getProperty(SCHEMA_NAME);
            final PropertyValue schemaBranchName = context.getProperty(SCHEMA_BRANCH_NAME);
            final PropertyValue schemaVersion = context.getProperty(SCHEMA_VERSION);
            return new SchemaNamePropertyStrategy(schemaRegistry, schemaName, schemaBranchName, schemaVersion);
        } else if (allowableValue.equalsIgnoreCase(INHERIT_RECORD_SCHEMA.getValue())) {
            return new InheritSchemaFromRecord();
        } else if (allowableValue.equalsIgnoreCase(SCHEMA_TEXT_PROPERTY.getValue())) {
            return new AvroSchemaTextStrategy(context.getProperty(SCHEMA_TEXT));
        } else if (allowableValue.equalsIgnoreCase(HWX_CONTENT_ENCODED_SCHEMA.getValue())) {
            return new HortonworksEncodedSchemaReferenceStrategy(schemaRegistry);
        } else if (allowableValue.equalsIgnoreCase(HWX_SCHEMA_REF_ATTRIBUTES.getValue())) {
            return new HortonworksAttributeSchemaReferenceStrategy(schemaRegistry);
        } else if (allowableValue.equalsIgnoreCase(CONFLUENT_ENCODED_SCHEMA.getValue())) {
            return new ConfluentSchemaRegistryStrategy(schemaRegistry);
        }

        return null;
    }

}
