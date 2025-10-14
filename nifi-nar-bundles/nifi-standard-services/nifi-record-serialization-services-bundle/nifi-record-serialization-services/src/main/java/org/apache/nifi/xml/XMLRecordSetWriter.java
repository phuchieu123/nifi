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

import org.apache.nifi.NullSuppression;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.DateTimeTextRecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Tags({"xml", "resultset", "writer", "serialize", "record", "recordset", "row"})
@CapabilityDescription("Ghi một RecordSet sang XML. Các bản ghi được bao bọc bởi một thẻ gốc.")
public class XMLRecordSetWriter extends DateTimeTextRecordSetWriter implements RecordSetWriterFactory {

    public static final AllowableValue ALWAYS_SUPPRESS = new AllowableValue("always-suppress", "Luôn ẩn",
            "Các trường bị thiếu (có trong lược đồ nhưng không có trong bản ghi), hoặc có giá trị null, sẽ không được ghi ra");
    public static final AllowableValue NEVER_SUPPRESS = new AllowableValue("never-suppress", "Không bao giờ ẩn",
            "Các trường bị thiếu (có trong lược đồ nhưng không có trong bản ghi), hoặc có giá trị null, sẽ được ghi ra dưới dạng giá trị null");
    public static final AllowableValue SUPPRESS_MISSING = new AllowableValue("suppress-missing", "Ẩn các giá trị bị thiếu",
            "Khi một trường có giá trị null, nó sẽ được ghi ra. Tuy nhiên, nếu một trường được định nghĩa trong lược đồ và không có trong bản ghi, trường đó sẽ không được ghi ra.");

    public static final AllowableValue USE_PROPERTY_AS_WRAPPER = new AllowableValue("use-property-as-wrapper", "Sử dụng thuộc tính làm trình bao bọc",
            "Giá trị của thuộc tính \"Tên thẻ mảng\" sẽ được sử dụng làm tên thẻ để bao bọc các phần tử của một mảng. Tên trường của trường mảng sẽ được sử dụng cho tên thẻ " +
                    "của các phần tử.");
    public static final AllowableValue USE_PROPERTY_FOR_ELEMENTS = new AllowableValue("use-property-for-elements", "Sử dụng thuộc tính cho các phần tử",
            "Giá trị của thuộc tính \"Tên thẻ mảng\" sẽ được sử dụng cho tên thẻ của các phần tử của một mảng. Tên trường của trường mảng sẽ được sử dụng làm tên thẻ " +
                    "để bao bọc các phần tử.");
    public static final AllowableValue NO_WRAPPING = new AllowableValue("no-wrapping", "Không bao bọc",
            "Các phần tử của một mảng sẽ không được bao bọc");

    public static final PropertyDescriptor SUPPRESS_NULLS = new PropertyDescriptor.Builder()
            .name("suppress_nulls")
            .displayName("Ẩn các giá trị Null")
            .description("Chỉ định cách trình ghi xử lý một trường null")
            .allowableValues(NEVER_SUPPRESS, ALWAYS_SUPPRESS, SUPPRESS_MISSING)
            .defaultValue(NEVER_SUPPRESS.getValue())
            .required(true)
            .build();

    public static final PropertyDescriptor PRETTY_PRINT_XML = new PropertyDescriptor.Builder()
            .name("pretty_print_xml")
            .displayName("In đẹp XML")
            .description("Chỉ định liệu XML có nên được in đẹp hay không")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    public static final PropertyDescriptor OMIT_XML_DECLARATION = new PropertyDescriptor.Builder()
            .name("omit_xml_declaration")
            .displayName("Bỏ qua khai báo XML")
            .description("Chỉ định có bao gồm khai báo XML hay không")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    public static final PropertyDescriptor ROOT_TAG_NAME = new PropertyDescriptor.Builder()
            .name("root_tag_name")
            .displayName("Tên của thẻ gốc")
            .description("Chỉ định tên của thẻ gốc XML bao bọc tập hợp bản ghi. Thuộc tính này phải được xác định nếu " +
                    "trình ghi được cho là sẽ ghi nhiều bản ghi trong một FlowFile duy nhất.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(false)
            .build();

    public static final PropertyDescriptor RECORD_TAG_NAME = new PropertyDescriptor.Builder()
            .name("record_tag_name")
            .displayName("Tên của thẻ bản ghi")
            .description("Chỉ định tên của thẻ bản ghi XML bao bọc các trường bản ghi. Nếu điều này không được đặt, trình ghi " +
                    "sẽ sử dụng tên bản ghi trong lược đồ.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(false)
            .build();

    public static final PropertyDescriptor ARRAY_WRAPPING = new PropertyDescriptor.Builder()
            .name("array_wrapping")
            .displayName("Bao bọc các phần tử của mảng")
            .description("Chỉ định cách trình ghi bao bọc các phần tử của các trường thuộc loại mảng")
            .allowableValues(USE_PROPERTY_AS_WRAPPER, USE_PROPERTY_FOR_ELEMENTS, NO_WRAPPING)
            .defaultValue(NO_WRAPPING.getValue())
            .required(true)
            .build();

    public static final PropertyDescriptor ARRAY_TAG_NAME = new PropertyDescriptor.Builder()
            .name("array_tag_name")
            .displayName("Tên thẻ mảng")
            .description("Tên của thẻ được sử dụng bởi thuộc tính \"Bao bọc các phần tử của mảng\" để ghi các mảng")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(false)
            .build();

    public static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
            .name("Bộ ký tự")
            .description("Bộ ký tự được sử dụng khi ghi dữ liệu vào FlowFile")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue("UTF-8")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(SUPPRESS_NULLS);
        properties.add(PRETTY_PRINT_XML);
        properties.add(OMIT_XML_DECLARATION);
        properties.add(ROOT_TAG_NAME);
        properties.add(RECORD_TAG_NAME);
        properties.add(ARRAY_WRAPPING);
        properties.add(ARRAY_TAG_NAME);
        properties.add(CHARACTER_SET);
        return properties;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        if (!validationContext.getProperty(ARRAY_WRAPPING).getValue().equals(NO_WRAPPING.getValue())) {
            if (!validationContext.getProperty(ARRAY_TAG_NAME).isSet()) {
                StringBuilder explanation = new StringBuilder()
                        .append("if property \'")
                        .append(ARRAY_WRAPPING.getName())
                        .append("\' is defined as \'")
                        .append(USE_PROPERTY_AS_WRAPPER.getDisplayName())
                        .append("\' or \'")
                        .append(USE_PROPERTY_FOR_ELEMENTS.getDisplayName())
                        .append("\' the property \'")
                        .append(ARRAY_TAG_NAME.getDisplayName())
                        .append("\' has to be set.");

                return Collections.singleton(new ValidationResult.Builder()
                        .subject(ARRAY_TAG_NAME.getName())
                        .valid(false)
                        .explanation(explanation.toString())
                        .build());
            }
        }
        return Collections.emptyList();
    }

    @Override
    public RecordSetWriter createWriter(final ComponentLog logger, final RecordSchema schema, final OutputStream out, final Map<String, String> variables) throws SchemaNotFoundException, IOException {
        final String nullSuppression = getConfigurationContext().getProperty(SUPPRESS_NULLS).getValue();
        final NullSuppression nullSuppressionEnum;
        if (nullSuppression.equals(ALWAYS_SUPPRESS.getValue())) {
            nullSuppressionEnum = NullSuppression.ALWAYS_SUPPRESS;
        } else if (nullSuppression.equals(NEVER_SUPPRESS.getValue())) {
            nullSuppressionEnum = NullSuppression.NEVER_SUPPRESS;
        } else {
            nullSuppressionEnum = NullSuppression.SUPPRESS_MISSING;
        }

        final boolean prettyPrint = getConfigurationContext().getProperty(PRETTY_PRINT_XML).getValue().equals("true");
        final boolean omitDeclaration = getConfigurationContext().getProperty(OMIT_XML_DECLARATION).getValue().equals("true");

        final String rootTagName = getConfigurationContext().getProperty(ROOT_TAG_NAME).isSet()
                ? getConfigurationContext().getProperty(ROOT_TAG_NAME).getValue() : null;
        final String recordTagName = getConfigurationContext().getProperty(RECORD_TAG_NAME).isSet()
                ? getConfigurationContext().getProperty(RECORD_TAG_NAME).getValue() : null;

        final String arrayWrapping = getConfigurationContext().getProperty(ARRAY_WRAPPING).getValue();
        final ArrayWrapping arrayWrappingEnum;
        if (arrayWrapping.equals(NO_WRAPPING.getValue())) {
            arrayWrappingEnum = ArrayWrapping.NO_WRAPPING;
        } else if (arrayWrapping.equals(USE_PROPERTY_AS_WRAPPER.getValue())) {
            arrayWrappingEnum = ArrayWrapping.USE_PROPERTY_AS_WRAPPER;
        } else {
            arrayWrappingEnum = ArrayWrapping.USE_PROPERTY_FOR_ELEMENTS;
        }

        final String arrayTagName;
        if (getConfigurationContext().getProperty(ARRAY_TAG_NAME).isSet()) {
            arrayTagName = getConfigurationContext().getProperty(ARRAY_TAG_NAME).getValue();
        } else {
            arrayTagName = null;
        }

        final String charSet = getConfigurationContext().getProperty(CHARACTER_SET).getValue();

        return new WriteXMLResult(schema, getSchemaAccessWriter(schema, variables),
                out, prettyPrint, omitDeclaration, nullSuppressionEnum, arrayWrappingEnum, arrayTagName, rootTagName, recordTagName, charSet,
                getDateFormat().orElse(null), getTimeFormat().orElse(null), getTimestampFormat().orElse(null));
    }
}
