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

import org.apache.commons.text.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.Collections;
import java.util.Arrays;
import java.util.ArrayList;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"csv", "attributes", "flowfile"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Generates a CSV representation of the input FlowFile Attributes. The resulting CSV " +
        "can be written to either a newly generated attribute named 'CSVAttributes' or written to the FlowFile as content.  " +
        "If the attribute value contains a comma, newline or double quote, then the attribute value will be " +
        "escaped with double quotes.  Any double quote characters in the attribute value are escaped with " +
        "another double quote.")
@WritesAttributes({
        @WritesAttribute(attribute = "CSVSchema", description = "CSV representation of the Schema"),
        @WritesAttribute(attribute = "CSVData", description = "CSV representation of Attributes")
})

public class AttributesToCSV extends AbstractProcessor {
    private static final String DATA_ATTRIBUTE_NAME = "CSVData";
    private static final String SCHEMA_ATTRIBUTE_NAME = "CSVSchema";
    private static final String OUTPUT_SEPARATOR = ",";
    private static final String OUTPUT_MIME_TYPE = "text/csv";
    private static final String SPLIT_REGEX = OUTPUT_SEPARATOR + "(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";

    static final AllowableValue OUTPUT_OVERWRITE_CONTENT = new AllowableValue("flowfile-content", "flowfile-content", "The resulting CSV string will be placed into the content of the flowfile." +
            "Existing flowfile context will be overwritten. 'CSVData' will not be written to at all (neither null nor empty string).");
    static final AllowableValue OUTPUT_NEW_ATTRIBUTE= new AllowableValue("flowfile-attribute", "flowfile-attribute", "The resulting CSV string will be placed into a new flowfile" +
            " attribute named 'CSVData'.  The content of the flowfile will not be changed.");

    public static final PropertyDescriptor ATTRIBUTES_LIST = new PropertyDescriptor.Builder()
            .name("attribute-list")
            .displayName("Danh sách thuộc tính")
           .description("Danh sách các thuộc tính được phân tách bằng dấu phẩy sẽ được đưa vào CSV kết quả. Nếu giá trị này để trống, tất cả các thuộc tính hiện có sẽ được bao gồm. Danh sách thuộc tính này phân biệt chữ hoa chữ thường và hỗ trợ các tên thuộc tính chứa dấu phẩy. Nếu một thuộc tính được chỉ định trong danh sách không được tìm thấy, nó sẽ được xuất ra CSV kết quả dưới dạng chuỗi rỗng hoặc null, tùy thuộc vào thuộc tính 'Null Value'. Nếu một thuộc tính cốt lõi được chỉ định trong danh sách và thuộc tính 'Include Core Attributes' là false, thuộc tính cốt lõi đó vẫn sẽ được bao gồm. Danh sách thuộc tính LUÔN luôn chiến thắng.")

            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor ATTRIBUTES_REGEX = new PropertyDescriptor.Builder()
            .name("attributes-regex")
            .displayName("Thuộc tính biểu thức chính quy")
            .description("Biểu thức chính quy sẽ được áp dụng lên các thuộc tính của flow file để chọn các thuộc tính khớp. Thuộc tính này có thể được sử dụng kết hợp với thuộc tính danh sách thuộc tính. Kết quả cuối cùng sẽ chứa sự kết hợp các kết quả tìm được từ ATTRIBUTE_LIST và ATTRIBUTE_REGEX.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.createRegexValidator(0, Integer.MAX_VALUE, true))
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    public static final PropertyDescriptor DESTINATION = new PropertyDescriptor.Builder()
            .name("destination")
          .displayName("Điểm đến")
.description("Kiểm soát việc giá trị CSV sẽ được ghi dưới dạng thuộc tính flowfile mới 'CSVData' " +
              "hay sẽ được ghi vào nội dung của flowfile.")

            .required(true)
            .allowableValues(OUTPUT_NEW_ATTRIBUTE, OUTPUT_OVERWRITE_CONTENT)
            .defaultValue(OUTPUT_NEW_ATTRIBUTE.getDisplayName())
            .build();

    public static final PropertyDescriptor INCLUDE_CORE_ATTRIBUTES = new PropertyDescriptor.Builder()
            .name("include-core-attributes")
           .displayName("Bao gồm thuộc tính Core")
.description("Xác định liệu các thuộc tính org.apache.nifi.flowfile.attributes.CoreAttributes, có mặt trong mọi FlowFile, " +
              "có nên được bao gồm trong giá trị CSV cuối cùng được tạo ra hay không. Các thuộc tính Core sẽ " +
              "được thêm vào cuối chuỗi CSVData và CSVSchema. Thuộc tính Danh sách Thuộc tính sẽ ghi đè cài đặt này.")

            .required(true)
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor NULL_VALUE_FOR_EMPTY_STRING = new PropertyDescriptor.Builder()
            .name("null-value")
           .displayName("Giá trị Null")
.description("Nếu đúng, một thuộc tính không tồn tại hoặc rỗng sẽ được gán là 'null' trong CSV kết quả. Nếu sai, một chuỗi rỗng " +
              "sẽ được đưa vào trong CSV.")

            .required(true)
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .build();
    public static final PropertyDescriptor INCLUDE_SCHEMA = new PropertyDescriptor.Builder()
            .name("include-schema")
            .displayName("Bao gồm các lược đồ")
          .description("Nếu được bật, sơ đồ (tên các thuộc tính) cũng sẽ được chuyển đổi thành một chuỗi CSV. Chuỗi này sẽ được áp dụng " +
              "vào một thuộc tính mới mang tên 'CSVSchema', hoặc sẽ được thêm vào dòng đầu tiên trong nội dung, tùy thuộc vào cài đặt của thuộc tính DESTINATION.")
            .required(true)
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("Đã chuyển đổi thành công các thuộc tính sang CSV").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("Không thể chuyển đổi thuộc tính sang CSV").build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    private volatile Boolean includeCoreAttributes;
    private volatile Set<String> coreAttributes;
    private volatile boolean destinationContent;
    private volatile boolean nullValForEmptyString;
    private volatile Pattern pattern;
    private volatile Boolean includeSchema;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(ATTRIBUTES_LIST);
        properties.add(ATTRIBUTES_REGEX);
        properties.add(DESTINATION);
        properties.add(INCLUDE_CORE_ATTRIBUTES);
        properties.add(NULL_VALUE_FOR_EMPTY_STRING);
        properties.add(INCLUDE_SCHEMA);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }


    private Map<String, String> buildAttributesMapForFlowFile(FlowFile ff, Set<String> attributes, Pattern attPattern) {
        Map<String, String> result;
        Map<String, String> ffAttributes = ff.getAttributes();
        result = new LinkedHashMap<>(ffAttributes.size());

        if (!attributes.isEmpty() || attPattern != null) {
            if (!attributes.isEmpty()) {
                //the user gave a list of attributes
                for (String attribute : attributes) {
                    String val = ff.getAttribute(attribute);
                    if (val != null && !val.isEmpty()) {
                        result.put(attribute, val);
                    } else {
                        if (nullValForEmptyString) {
                            result.put(attribute, "null");
                        } else {
                            result.put(attribute, "");
                        }
                    }
                }
            }
            if(attPattern != null) {
                for (Map.Entry<String, String> e : ff.getAttributes().entrySet()) {
                    if(attPattern.matcher(e.getKey()).matches()) {
                        result.put(e.getKey(), e.getValue());
                    }
                }
            }
        } else {
            //the user did not give a list of attributes, take all the attributes from the flowfile
            result.putAll(ffAttributes);
        }

        //now glue on the core attributes if the user wants them.
        if(includeCoreAttributes) {
            for (String coreAttribute : coreAttributes) {
                //make sure this coreAttribute is applicable to this flowfile.
                String val = ff.getAttribute(coreAttribute);
                if(ffAttributes.containsKey(coreAttribute)) {
                    if (!StringUtils.isEmpty(val)){
                        result.put(coreAttribute, val);
                    } else {
                        if (nullValForEmptyString) {
                            result.put(coreAttribute, "null");
                        } else {
                            result.put(coreAttribute, "");
                        }
                    }
                }
            }
        } else {
            //remove core attributes since the user does not want them, unless they are in the attribute list. Attribute List always wins
            for (String coreAttribute : coreAttributes) {
                //never override user specified attributes, even if the user has selected to exclude core attributes
                if(!attributes.contains(coreAttribute)) {
                    result.remove(coreAttribute);
                }
            }
        }
        return result;
    }

    private LinkedHashSet<String> attributeListStringToSet(String attributeList) {
        //take the user specified attribute list string and convert to list of strings.
        LinkedHashSet<String> result = new LinkedHashSet<>();
        if (StringUtils.isNotBlank(attributeList)) {
            String[] ats = attributeList.split(SPLIT_REGEX);
            for (String str : ats) {
                result.add(StringEscapeUtils.unescapeCsv(str.trim()));
            }
        }
        return result;
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        includeCoreAttributes = context.getProperty(INCLUDE_CORE_ATTRIBUTES).asBoolean();
        coreAttributes = Arrays.stream(CoreAttributes.values()).map(CoreAttributes::key).collect(Collectors.toCollection(LinkedHashSet::new));
        destinationContent = OUTPUT_OVERWRITE_CONTENT.getValue().equals(context.getProperty(DESTINATION).getValue());
        nullValForEmptyString = context.getProperty(NULL_VALUE_FOR_EMPTY_STRING).asBoolean();
        includeSchema = context.getProperty(INCLUDE_SCHEMA).asBoolean();
     }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }
        if(context.getProperty(ATTRIBUTES_REGEX).isSet()) {
            pattern = Pattern.compile(context.getProperty(ATTRIBUTES_REGEX).evaluateAttributeExpressions(original).getValue());
        }

        final Set<String> attributeList = attributeListStringToSet(context.getProperty(ATTRIBUTES_LIST).evaluateAttributeExpressions(original).getValue());
        final Map<String, String> atrList = buildAttributesMapForFlowFile(original, attributeList, pattern);

        //escape attribute values
        int index = 0;
        final int atrListSize = atrList.values().size() -1;
        final StringBuilder sbValues = new StringBuilder();
        for (final Map.Entry<String,String> attr : atrList.entrySet()) {
            sbValues.append(StringEscapeUtils.escapeCsv(attr.getValue()));
            sbValues.append(index++ < atrListSize ? OUTPUT_SEPARATOR : "");
        }

        //build the csv header if needed
        final StringBuilder sbNames = new StringBuilder();
        if(includeSchema){
            index = 0;
            for (final Map.Entry<String,String> attr : atrList.entrySet()) {
                sbNames.append(StringEscapeUtils.escapeCsv(attr.getKey()));
                sbNames.append(index++ < atrListSize ? OUTPUT_SEPARATOR : "");
            }
        }

        try {
            if (destinationContent) {
                FlowFile conFlowfile = session.write(original, (in, out) -> {
                        if(includeSchema){
                            sbNames.append(System.getProperty("line.separator"));
                            out.write(sbNames.toString().getBytes());
                        }
                        out.write(sbValues.toString().getBytes());
                });
                conFlowfile = session.putAttribute(conFlowfile, CoreAttributes.MIME_TYPE.key(), OUTPUT_MIME_TYPE);
                session.transfer(conFlowfile, REL_SUCCESS);
            } else {
                FlowFile atFlowfile = session.putAttribute(original, DATA_ATTRIBUTE_NAME , sbValues.toString());
                if(includeSchema){
                    session.putAttribute(original, SCHEMA_ATTRIBUTE_NAME , sbNames.toString());
                }
                session.transfer(atFlowfile, REL_SUCCESS);
            }
        } catch (Exception e) {
            getLogger().error(e.getMessage());
            session.transfer(original, REL_FAILURE);
        }
    }
}