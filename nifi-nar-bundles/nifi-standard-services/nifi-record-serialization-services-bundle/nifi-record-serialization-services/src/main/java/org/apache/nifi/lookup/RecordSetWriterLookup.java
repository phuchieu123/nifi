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

package org.apache.nifi.lookup;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Tags({"lookup", "result", "set", "writer", "serializer", "record", "recordset", "row"})
@SeeAlso({ReaderLookup.class})
@CapabilityDescription("Cung cấp một RecordSetWriterFactory có thể được sử dụng để chọn động một RecordSetWriterFactory khác. " +
        "Điều này cho phép nhiều RecordSetWriterFactory được định nghĩa và đăng ký, sau đó được chọn động tại thời điểm chạy bằng cách gán thuộc tính cho FlowFiles và tham chiếu các thuộc tính đó trong trường 'Service to Use'.")
@DynamicProperty(
        name = "Tên của RecordSetWriter",
        value = "Một controller service RecordSetWriterFactory",
        expressionLanguageScope = ExpressionLanguageScope.NONE,
        description = ""
)
public class RecordSetWriterLookup extends AbstractControllerService implements RecordSetWriterFactory {

    static final PropertyDescriptor SERVICE_TO_USE = new PropertyDescriptor.Builder()
        .name("Service to Use")
        .displayName("Service to Use")
        .description("Chỉ định tên của thuộc tính do người dùng định nghĩa mà dịch vụ Controller liên kết sẽ được sử dụng.")
        .required(true)
        .defaultValue("${recordsetwriter.name}")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    private volatile Map<String, RecordSetWriterFactory> recordSetWriterFactoryMap;
    private volatile PropertyValue serviceToUseValue;

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .description("RecordSetWriterFactory sẽ được trả về khi '" + propertyDescriptorName + "' là RecordSetWriter được chọn")
                .identifiesControllerService(RecordSetWriterFactory.class)
                .build();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.singletonList(SERVICE_TO_USE);
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> results = new ArrayList<>();

        final Set<String> serviceNames = new HashSet<>();
        for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
            if (descriptor.isDynamic()) {
                serviceNames.add(descriptor.getName());
            }

            final String referencedId = context.getProperty(descriptor).getValue();
            if (this.getIdentifier().equals(referencedId)) {
                results.add(new ValidationResult.Builder()
                    .subject(descriptor.getDisplayName())
                    .explanation("Dịch vụ hiện tại không thể được đăng ký làm RecordSetWriter để lookup")
                    .valid(false)
                    .build());
            }
        }

        if (serviceNames.isEmpty()) {
            results.add(new ValidationResult.Builder()
                .subject(this.getClass().getSimpleName())
                .explanation("Ít nhất một RecordSetWriter phải được định nghĩa thông qua các dynamic property")
                .valid(false)
                .build());
        }

        final PropertyValue serviceToUseValue = context.getProperty(SERVICE_TO_USE);
        if (!serviceToUseValue.isExpressionLanguagePresent()) {
            final String selectedValue = serviceToUseValue.getValue();
            if (!serviceNames.contains(selectedValue)) {
                results.add(new ValidationResult.Builder()
                    .subject(SERVICE_TO_USE.getDisplayName())
                    .explanation("Không có dịch vụ nào được định nghĩa với tên <" + selectedValue + ">")
                    .valid(false)
                    .build());
            }
        }

        return results;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        final Map<String,RecordSetWriterFactory> serviceMap = new HashMap<>();

        for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
            if (descriptor.isDynamic()) {
                final RecordSetWriterFactory recordSetWriterFactory = context.getProperty(descriptor).asControllerService(RecordSetWriterFactory.class);
                serviceMap.put(descriptor.getName(), recordSetWriterFactory);
            }
        }

        recordSetWriterFactoryMap = Collections.unmodifiableMap(serviceMap);
        serviceToUseValue = context.getProperty(SERVICE_TO_USE);
    }


    @Override
    public RecordSchema getSchema(Map<String, String> variables, RecordSchema readSchema) throws SchemaNotFoundException, IOException {
        return getRecordSetWriterFactory(variables).getSchema(variables, readSchema);
    }

    @Override
    public RecordSetWriter createWriter(ComponentLog logger, RecordSchema schema, OutputStream out, Map<String, String> variables) throws SchemaNotFoundException, IOException {
        return getRecordSetWriterFactory(variables).createWriter(logger, schema, out, variables);
    }

    private RecordSetWriterFactory getRecordSetWriterFactory(Map<String, String> variables){
        final String serviceName = serviceToUseValue.evaluateAttributeExpressions(variables).getValue();
        if (serviceName.trim().isEmpty()) {
            throw new ProcessException("Unable to determine which Record Writer to use: after evaluating the property value against supplied variables, got an empty value");
        }

        final RecordSetWriterFactory recordSetWriterFactory = recordSetWriterFactoryMap.get(serviceName);
        if (recordSetWriterFactory == null) {
            throw new ProcessException("No Record Writer was configured with the name <" + serviceName + ">");
        }

        return recordSetWriterFactory;
    }
}