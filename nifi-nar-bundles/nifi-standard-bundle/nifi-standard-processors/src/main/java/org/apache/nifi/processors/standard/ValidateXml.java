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

import org.apache.nifi.annotation.behavior.EventDriven;
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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.xml.processing.ProcessingException;
import org.apache.nifi.xml.processing.stream.StandardXMLStreamReaderProvider;
import org.apache.nifi.xml.processing.stream.XMLStreamReaderProvider;
import org.apache.nifi.xml.processing.validation.StandardSchemaValidator;
import org.apache.nifi.xml.processing.validation.SchemaValidator;
import org.xml.sax.SAXException;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.Source;
import javax.xml.transform.stax.StAXSource;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"xml", "schema", "validation", "xsd"})
@WritesAttributes({
    @WritesAttribute(attribute = "validatexml.invalid.error", description = "Nếu flow file được chuyển đến mối quan hệ không hợp lệ "
            + "thuộc tính này sẽ chứa thông báo lỗi do xác thực thất bại.")
})
@CapabilityDescription("Xác thực XML chứa trong một FlowFile. Theo mặc định, XML nằm trong nội dung của FlowFile. Nếu thuộc tính 'Thuộc tính Nguồn XML' (XML Source Attribute) được đặt, XML cần xác thực "
        + "sẽ nằm trong thuộc tính được chỉ định. Không khuyến khích sử dụng thuộc tính để chứa các tài liệu XML lớn; làm như vậy có thể ảnh hưởng xấu đến hiệu suất hệ thống. "
        + "Việc xác thực lược đồ đầy đủ sẽ được thực hiện nếu bộ xử lý được cấu hình với chi tiết lược đồ XSD. Nếu không, việc xác thực duy nhất được thực hiện là "
        + "để đảm bảo cú pháp XML là chính xác và được định dạng tốt, ví dụ: tất cả các thẻ mở đều được đóng đúng cách.")
@SystemResourceConsideration(resource = SystemResource.MEMORY, description = "Mặc dù bộ xử lý này hỗ trợ xử lý XML trong các thuộc tính, nhưng việc chứa "
        + "một lượng lớn dữ liệu trong thuộc tính là không được khuyến khích. Nói chung, giá trị thuộc tính nên càng nhỏ càng tốt và không chứa quá vài trăm ký tự.")
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.REFERENCE_REMOTE_RESOURCES,
                        explanation = "Cấu hình lược đồ có thể tham chiếu đến các tài nguyên qua HTTP"
                )
        }
)
public class ValidateXml extends AbstractProcessor {

    public static final String ERROR_ATTRIBUTE_KEY = "validatexml.invalid.error";

    public static final PropertyDescriptor SCHEMA_FILE = new PropertyDescriptor.Builder()
            .name("Schema File")
            .displayName("Tệp Lược đồ")
            .description("Đường dẫn tệp hoặc URL đến tệp Lược đồ XSD sẽ được sử dụng để xác thực. Nếu thuộc tính này để trống, chỉ cú pháp/cấu trúc XML sẽ được xác thực.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE, ResourceType.URL)
            .build();
    public static final PropertyDescriptor XML_SOURCE_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("XML Source Attribute")
            .displayName("Thuộc tính Nguồn XML")
            .description("Tên của thuộc tính chứa XML cần được xác thực. Nếu thuộc tính này để trống, nội dung của FlowFile sẽ được xác thực.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.ATTRIBUTE_KEY_VALIDATOR)
            .build();

    public static final Relationship REL_VALID = new Relationship.Builder()
            .name("valid")
            .description("Các FlowFile được xác thực thành công dựa trên lược đồ (nếu được cung cấp), hoặc được xác minh là XML được định dạng tốt sẽ được chuyển đến mối quan hệ này")
            .build();
    public static final Relationship REL_INVALID = new Relationship.Builder()
            .name("invalid")
            .description("Các FlowFile không hợp lệ theo lược đồ đã chỉ định hoặc chứa XML không hợp lệ sẽ được chuyển đến mối quan hệ này")
            .build();
    private static final String SCHEMA_LANGUAGE = "http://www.w3.org/2001/XMLSchema";

    private static final SchemaValidator SCHEMA_VALIDATOR = new StandardSchemaValidator();

    private static final XMLStreamReaderProvider READER_PROVIDER = new StandardXMLStreamReaderProvider();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    private final AtomicReference<Schema> schemaRef = new AtomicReference<>();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(SCHEMA_FILE);
        properties.add(XML_SOURCE_ATTRIBUTE);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_VALID);
        relationships.add(REL_INVALID);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnScheduled
    public void parseSchema(final ProcessContext context) throws SAXException {
        if (context.getProperty(SCHEMA_FILE).isSet()) {
            final URL url = context.getProperty(SCHEMA_FILE).evaluateAttributeExpressions().asResource().asURL();
            final SchemaFactory schemaFactory = SchemaFactory.newInstance(SCHEMA_LANGUAGE);
            final Schema schema = schemaFactory.newSchema(url);
            schemaRef.set(schema);
        } else {
            schemaRef.set(null);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final List<FlowFile> flowFiles = session.get(50);
        if (flowFiles.isEmpty()) {
            return;
        }

        final ComponentLog logger = getLogger();
        final boolean attributeContainsXML = context.getProperty(XML_SOURCE_ATTRIBUTE).isSet();

        for (FlowFile flowFile : flowFiles) {
            final AtomicBoolean valid = new AtomicBoolean(true);
            final AtomicReference<Exception> exception = new AtomicReference<>(null);

            try {
                if (attributeContainsXML) {
                    // If XML source attribute is set, validate attribute value
                    String xml = flowFile.getAttribute(context.getProperty(XML_SOURCE_ATTRIBUTE).evaluateAttributeExpressions().getValue());
                    ByteArrayInputStream inputStream = new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));

                    validate(inputStream);
                } else {
                    // If XML source attribute is not set, validate flowfile content
                    session.read(flowFile, inputStream -> validate(inputStream));
                }
            } catch (final RuntimeException e) {
                valid.set(false);
                exception.set(e);
            }

            // determine source location of XML for logging purposes
            String xmlSource = attributeContainsXML ? "attribute '" + context.getProperty(XML_SOURCE_ATTRIBUTE).evaluateAttributeExpressions().getValue() + "'" : "content";
            if (valid.get()) {
                if (context.getProperty(SCHEMA_FILE).isSet()) {
                    logger.debug("Successfully validated XML in {} of {} against schema; routing to 'valid'", xmlSource, flowFile);
                } else {
                    logger.debug("Successfully validated XML is well-formed in {} of {}; routing to 'valid'", xmlSource, flowFile);
                }
                session.getProvenanceReporter().route(flowFile, REL_VALID);
                session.transfer(flowFile, REL_VALID);
            } else {
                flowFile = session.putAttribute(flowFile, ERROR_ATTRIBUTE_KEY, exception.get().getLocalizedMessage());
                if (context.getProperty(SCHEMA_FILE).isSet()) {
                    logger.info("Failed to validate XML in {} of {} against schema due to {}; routing to 'invalid'", xmlSource, flowFile, exception.get().getLocalizedMessage());
                } else {
                    logger.info("Failed to validate XML is well-formed in {} of {} due to {}; routing to 'invalid'", xmlSource, flowFile, exception.get().getLocalizedMessage());
                }
                session.getProvenanceReporter().route(flowFile, REL_INVALID);
                session.transfer(flowFile, REL_INVALID);
            }
        }
    }

    private void validate(final InputStream in) {
        final Schema schema = schemaRef.get();
        if (schema == null) {
            // Parse Document without schema validation
            final XMLStreamReader reader = READER_PROVIDER.getStreamReader(new StreamSource(in));
            try {
                while (reader.hasNext()) {
                    reader.next();
                }
            } catch (final XMLStreamException e) {
                throw new ProcessingException("Reading stream failed: " + e.getMessage(), e);
            }
        } else {
            final XMLStreamReaderProvider readerProvider = new StandardXMLStreamReaderProvider();
            final XMLStreamReader reader = readerProvider.getStreamReader(new StreamSource(in));
            final Source source = new StAXSource(reader);
            SCHEMA_VALIDATOR.validate(schema, source);
        }
    }
}
