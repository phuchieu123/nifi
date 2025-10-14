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

import static javax.xml.xpath.XPathConstants.NODESET;
import static javax.xml.xpath.XPathConstants.STRING;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.xml.namespace.QName;
import javax.xml.transform.Source;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.SystemResourceConsiderations;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.standard.xml.DocumentTypeAllowedDocumentProvider;
import org.apache.nifi.xml.processing.ProcessingException;
import org.apache.nifi.xml.processing.parsers.StandardDocumentProvider;
import org.apache.nifi.xml.processing.transform.StandardTransformProvider;
import org.w3c.dom.Document;

import net.sf.saxon.xpath.XPathEvaluator;
import net.sf.saxon.xpath.XPathFactoryImpl;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"XML", "evaluate", "XPath"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Đánh giá một hoặc nhiều biểu thức XPath đối với nội dung của FlowFile. Kết quả của các biểu thức XPath này "
        + "sẽ được gán vào các thuộc tính (Attributes) của FlowFile hoặc được ghi trực tiếp vào nội dung của FlowFile, tùy theo cấu hình "
        + "của Processor. Các biểu thức XPath được nhập thông qua việc thêm các thuộc tính do người dùng định nghĩa; tên của thuộc tính "
        + "sẽ ánh xạ tới tên Attribute mà kết quả sẽ được gán vào (nếu Destination là flowfile-attribute; nếu không, tên thuộc tính sẽ bị bỏ qua). "
        + "Giá trị của thuộc tính phải là một biểu thức XPath hợp lệ. Nếu biểu thức XPath trả về nhiều hơn một node và kiểu trả về (Return Type) "
        + "được đặt là 'nodeset' (trực tiếp hoặc thông qua 'auto-detect' với Destination là 'flowfile-content'), FlowFile sẽ không bị thay đổi "
        + "và sẽ được định tuyến tới 'failure'. Nếu biểu thức XPath không trả về bất kỳ Node nào, FlowFile sẽ được định tuyến tới 'unmatched' "
        + "mà không làm thay đổi nội dung của nó. Nếu Destination là flowfile-attribute và biểu thức không khớp với gì, các thuộc tính sẽ được "
        + "tạo với giá trị là chuỗi rỗng, và FlowFile luôn được định tuyến tới 'matched'.")
@WritesAttribute(attribute = "user-defined", description = "Processor này sẽ thêm các thuộc tính do người dùng định nghĩa nếu thuộc tính <Destination> được đặt thành flowfile-attribute.")
@DynamicProperty(name = "Một thuộc tính FlowFile (nếu <Destination> được đặt là 'flowfile-attribute')",
        value = "Một biểu thức XPath",
        description = "Nếu <Destination>='flowfile-attribute' thì thuộc tính FlowFile sẽ được gán bằng kết quả của biểu thức XPath. "
        + "Nếu <Destination>='flowfile-content' thì nội dung FlowFile sẽ được gán bằng kết quả của biểu thức XPath.")
@SystemResourceConsiderations({
        @SystemResourceConsideration(resource = SystemResource.MEMORY, description = "Việc xử lý yêu cầu đọc toàn bộ nội dung FlowFile vào bộ nhớ")
})

public class EvaluateXPath extends AbstractProcessor {

    public static final String DESTINATION_ATTRIBUTE = "flowfile-attribute";
    public static final String DESTINATION_CONTENT = "flowfile-content";
    public static final String RETURN_TYPE_AUTO = "auto-detect";
    public static final String RETURN_TYPE_NODESET = "nodeset";
    public static final String RETURN_TYPE_STRING = "string";

    public static final PropertyDescriptor DESTINATION = new PropertyDescriptor.Builder()
            .name("Destination")
            .description("Chỉ ra liệu kết quả của việc đánh giá XPath sẽ được ghi vào nội dung của FlowFile hay vào một thuộc tính (attribute) của FlowFile; "
            + "nếu sử dụng thuộc tính (attribute), phải chỉ định thuộc tính 'Attribute Name'. Nếu được đặt là flowfile-content, "
            + "chỉ được phép khai báo một biểu thức XPath duy nhất, và tên thuộc tính (property name) sẽ bị bỏ qua.")

            .required(true)
            .allowableValues(DESTINATION_CONTENT, DESTINATION_ATTRIBUTE)
            .defaultValue(DESTINATION_CONTENT)
            .build();

   public static final PropertyDescriptor RETURN_TYPE = new PropertyDescriptor.Builder()
        .name("Return Type")
        .description("Chỉ định kiểu dữ liệu trả về mong muốn của các biểu thức XPath. Việc chọn 'auto-detect' sẽ tự động đặt kiểu trả về là 'nodeset' "
                + "khi Destination là 'flowfile-content', và 'string' khi Destination là 'flowfile-attribute'.")
        .required(true)
        .allowableValues(RETURN_TYPE_AUTO, RETURN_TYPE_NODESET, RETURN_TYPE_STRING)
        .defaultValue(RETURN_TYPE_AUTO)
        .build();

public static final PropertyDescriptor VALIDATE_DTD = new PropertyDescriptor.Builder()
        .displayName("Allow DTD")
        .name("Validate DTD")
        .description("Cho phép sử dụng khai báo Document Type Declaration (DTD) nhúng trong XML. "
                + "Tính năng này nên bị vô hiệu hóa để tránh các lỗ hổng bảo mật do mở rộng thực thể XML (XML entity expansion).")
        .required(true)
        .allowableValues("true", "false")
        .defaultValue("false")
        .build();

public static final Relationship REL_MATCH = new Relationship.Builder()
        .name("matched")
        .description("FlowFile sẽ được chuyển đến mối quan hệ này "
                + "khi biểu thức XPath được đánh giá thành công và FlowFile được sửa đổi dựa trên kết quả đó.")
        .build();

public static final Relationship REL_NO_MATCH = new Relationship.Builder()
        .name("unmatched")
        .description("FlowFile sẽ được chuyển đến mối quan hệ này "
                + "khi biểu thức XPath không khớp với nội dung của FlowFile và Destination được đặt là flowfile-content.")
        .build();

public static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("FlowFile sẽ được chuyển đến mối quan hệ này "
                + "khi biểu thức XPath không thể được đánh giá với nội dung của FlowFile; ví dụ, nếu FlowFile không phải XML hợp lệ, "
                + "hoặc khi kiểu trả về (Return Type) là 'nodeset' nhưng XPath trả về nhiều nút (multiple nodes).")
        .build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;

    private final AtomicReference<XPathFactory> factoryRef = new AtomicReference<>();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_MATCH);
        relationships.add(REL_NO_MATCH);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DESTINATION);
        properties.add(RETURN_TYPE);
        properties.add(VALIDATE_DTD);
        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(context));

        final String destination = context.getProperty(DESTINATION).getValue();
        if (DESTINATION_CONTENT.equals(destination)) {
            int xpathCount = 0;

            for (final PropertyDescriptor desc : context.getProperties().keySet()) {
                if (desc.isDynamic()) {
                    xpathCount++;
                }
            }

            if (xpathCount != 1) {
                results.add(new ValidationResult.Builder().subject("XPaths").valid(false)
                        .explanation("Exactly one XPath must be set if using destination of " + DESTINATION_CONTENT).build());
            }
        }

        return results;
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
    public void initializeXPathFactory() {
        factoryRef.set(new XPathFactoryImpl());
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(ExpressionLanguageScope.NONE)
                .addValidator(new XPathValidator())
                .required(false)
                .dynamic(true)
                .build();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final List<FlowFile> flowFiles = session.get(50);
        if (flowFiles.isEmpty()) {
            return;
        }

        final ComponentLog logger = getLogger();

        final XPathFactory factory = factoryRef.get();
        final XPathEvaluator xpathEvaluator = (XPathEvaluator) factory.newXPath();
        final Map<String, XPathExpression> attributeToXPathMap = new HashMap<>();

        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (!entry.getKey().isDynamic()) {
                continue;
            }
            final XPathExpression xpathExpression;
            try {
                xpathExpression = xpathEvaluator.compile(entry.getValue());
                attributeToXPathMap.put(entry.getKey().getName(), xpathExpression);
            } catch (XPathExpressionException e) {
                throw new ProcessException(e);  // should not happen because we've already validated the XPath (in XPathValidator)
            }
        }

        final String destination = context.getProperty(DESTINATION).getValue();
        final QName returnType;

        switch (context.getProperty(RETURN_TYPE).getValue()) {
            case RETURN_TYPE_AUTO:
                if (DESTINATION_ATTRIBUTE.equals(destination)) {
                    returnType = STRING;
                } else if (DESTINATION_CONTENT.equals(destination)) {
                    returnType = NODESET;
                } else {
                    throw new IllegalStateException("The only possible destinations should be CONTENT or ATTRIBUTE...");
                }
                break;
            case RETURN_TYPE_NODESET:
                returnType = NODESET;
                break;
            case RETURN_TYPE_STRING:
                returnType = STRING;
                break;
            default:
                throw new IllegalStateException("There are no other return types...");
        }

        final boolean validatingDeclaration = context.getProperty(VALIDATE_DTD).asBoolean();

        final StandardTransformProvider transformProvider = new StandardTransformProvider();
        transformProvider.setIndent(true);
        flowFileLoop:
        for (FlowFile flowFile : flowFiles) {
            final AtomicReference<Throwable> error = new AtomicReference<>(null);
            final AtomicReference<Source> sourceRef = new AtomicReference<>(null);

            try {
                session.read(flowFile, rawIn -> {
                    try (final InputStream in = new BufferedInputStream(rawIn)) {
                        final StandardDocumentProvider documentProvider = validatingDeclaration
                                ? new DocumentTypeAllowedDocumentProvider()
                                : new StandardDocumentProvider();
                        final Document document = documentProvider.parse(in);
                        sourceRef.set(new DOMSource(document));
                    }
                });
            } catch (final Exception e) {
                logger.error("Input parsing failed {}", flowFile, e);
                session.transfer(flowFile, REL_FAILURE);
                continue;
            }

            final Map<String, String> xpathResults = new HashMap<>();

            for (final Map.Entry<String, XPathExpression> entry : attributeToXPathMap.entrySet()) {
                Object result;
                try {
                    result = entry.getValue().evaluate(sourceRef.get(), returnType);
                    if (result == null) {
                        continue;
                    }
                } catch (final XPathExpressionException e) {
                    logger.error("XPath Property [{}] evaluation on {} failed", flowFile, entry.getKey(), e);
                    session.transfer(flowFile, REL_FAILURE);
                    continue flowFileLoop;
                }

                if (returnType == NODESET) {
                    final NodeList nodeList = (NodeList) result;
                    if (nodeList.getLength() == 0) {
                        logger.info("XPath evaluation on {} produced no results", flowFile);
                        session.transfer(flowFile, REL_NO_MATCH);
                        continue flowFileLoop;
                    } else if (nodeList.getLength() > 1) {
                        logger.error("XPath evaluation on {} produced unexpected results [{}]", flowFile, nodeList.getLength());
                        session.transfer(flowFile, REL_FAILURE);
                        continue flowFileLoop;
                    }
                    final Node firstNode = nodeList.item(0);
                    final Source sourceNode = new DOMSource(firstNode);

                    if (DESTINATION_ATTRIBUTE.equals(destination)) {
                        try {
                            ByteArrayOutputStream baos = new ByteArrayOutputStream();
                            final StreamResult streamResult = new StreamResult(baos);
                            transformProvider.transform(sourceNode, streamResult);
                            xpathResults.put(entry.getKey(), new String(baos.toByteArray(), StandardCharsets.UTF_8));
                        } catch (final ProcessingException e) {
                            error.set(e);
                        }

                    } else if (DESTINATION_CONTENT.equals(destination)) {
                        flowFile = session.write(flowFile, rawOut -> {
                            try (final OutputStream out = new BufferedOutputStream(rawOut)) {
                                final StreamResult streamResult = new StreamResult(out);
                                transformProvider.transform(sourceNode, streamResult);
                            } catch (final ProcessingException e) {
                                error.set(e);
                            }
                        });
                    }

                } else {
                    final String resultString = (String) result;

                    if (DESTINATION_ATTRIBUTE.equals(destination)) {
                        xpathResults.put(entry.getKey(), resultString);
                    } else if (DESTINATION_CONTENT.equals(destination)) {
                        flowFile = session.write(flowFile, rawOut -> {
                            try (final OutputStream out = new BufferedOutputStream(rawOut)) {
                                out.write(resultString.getBytes(StandardCharsets.UTF_8));
                            }
                        });
                    }
                }
            }

            if (error.get() == null) {
                if (DESTINATION_ATTRIBUTE.equals(destination)) {
                    flowFile = session.putAllAttributes(flowFile, xpathResults);
                    final Relationship destRel = xpathResults.isEmpty() ? REL_NO_MATCH : REL_MATCH;
                    logger.info("XPath evaluation on {} completed with results [{}]: content updated", flowFile, xpathResults.size());
                    session.transfer(flowFile, destRel);
                    session.getProvenanceReporter().modifyAttributes(flowFile);
                } else if (DESTINATION_CONTENT.equals(destination)) {
                    logger.info("XPath evaluation on {} completed: content updated", flowFile);
                    session.transfer(flowFile, REL_MATCH);
                    session.getProvenanceReporter().modifyContent(flowFile);
                }
            } else {
                logger.error("XPath evaluation on {} failed", flowFile, error.get());
                session.transfer(flowFile, REL_FAILURE);
            }
        }
    }

    private static class XPathValidator implements Validator {

        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext validationContext) {
            try {
                XPathFactory factory = new XPathFactoryImpl();
                final XPathEvaluator evaluator = (XPathEvaluator) factory.newXPath();

                String error = null;
                try {
                    evaluator.compile(input);
                } catch (final Exception e) {
                    error = e.toString();
                }

                return new ValidationResult.Builder().input(input).subject(subject).valid(error == null).explanation(error).build();
            } catch (final Exception e) {
                return new ValidationResult.Builder().input(input).subject(subject).valid(false)
                        .explanation("Unable to initialize XPath engine due to " + e).build();
            }
        }
    }
}
