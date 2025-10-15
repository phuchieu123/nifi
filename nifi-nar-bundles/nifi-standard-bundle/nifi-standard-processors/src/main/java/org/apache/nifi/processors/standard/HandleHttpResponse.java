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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletResponse;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.http.HttpContextMap;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.util.HTTPUtils;
import org.apache.nifi.util.StopWatch;

@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"http", "https", "response", "egress", "web service"})
@CapabilityDescription("Gửi một Phản hồi HTTP đến Người yêu cầu đã tạo ra một FlowFile. Bộ xử lý này được thiết kế để sử dụng cùng với "
        + "HandleHttpRequest để tạo một dịch vụ web.")
@DynamicProperty(name = "Tên tiêu đề HTTP", value = "Giá trị tiêu đề HTTP",
                    description = "Các Tiêu đề HTTP này được đặt trong Phản hồi HTTP",
                    expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
@ReadsAttributes({
    @ReadsAttribute(attribute = HTTPUtils.HTTP_CONTEXT_ID, description = "Giá trị của thuộc tính này được sử dụng để tra cứu Phản hồi HTTP để "
        + "thông điệp phù hợp có thể được gửi lại cho người yêu cầu. Nếu thuộc tính này bị thiếu, FlowFile sẽ được chuyển đến 'thất bại'."),
    @ReadsAttribute(attribute = HTTPUtils.HTTP_REQUEST_URI, description = "Giá trị của URI được yêu cầu bởi client. Được sử dụng cho sự kiện provenance."),
    @ReadsAttribute(attribute = HTTPUtils.HTTP_REMOTE_HOST, description = "Địa chỉ IP của client. Được sử dụng cho sự kiện provenance."),
    @ReadsAttribute(attribute = HTTPUtils.HTTP_LOCAL_NAME, description = "Địa chỉ IP/tên máy chủ của máy chủ. Được sử dụng cho sự kiện provenance."),
    @ReadsAttribute(attribute = HTTPUtils.HTTP_PORT, description = "Cổng lắng nghe của máy chủ. Được sử dụng cho sự kiện provenance."),
    @ReadsAttribute(attribute = HTTPUtils.HTTP_SSL_CERT, description = "Tên phân biệt SSL (nếu có). Được sử dụng cho sự kiện provenance.")})
@SeeAlso(value = {HandleHttpRequest.class}, classNames = {"org.apache.nifi.http.StandardHttpContextMap"})
public class HandleHttpResponse extends AbstractProcessor {

    public static final PropertyDescriptor STATUS_CODE = new PropertyDescriptor.Builder()
            .name("Mã Trạng thái HTTP")
            .description("Mã Trạng thái HTTP để sử dụng khi phản hồi Yêu cầu HTTP. Xem Mục 10 của RFC 2616 để biết thêm thông tin.")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor HTTP_CONTEXT_MAP = new PropertyDescriptor.Builder()
            .name("Bản đồ Ngữ cảnh HTTP")
            .description("Dịch vụ Điều khiển Bản đồ Ngữ cảnh HTTP để sử dụng cho việc lưu vào bộ nhớ đệm Thông tin Yêu cầu HTTP")
            .required(true)
            .identifiesControllerService(HttpContextMap.class)
            .build();
    public static final PropertyDescriptor ATTRIBUTES_AS_HEADERS_REGEX = new PropertyDescriptor.Builder()
            .name("Các thuộc tính cần thêm vào Phản hồi HTTP (Regex)")
            .description("Chỉ định Biểu thức Chính quy xác định tên của các thuộc tính FlowFile cần được thêm vào phản hồi HTTP")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .required(false)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("thành công")
            .description("Các FlowFile sẽ được chuyển đến Mối quan hệ này sau khi phản hồi đã được gửi thành công đến người yêu cầu")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("thất bại")
            .description("Các FlowFile sẽ được chuyển đến Mối quan hệ này nếu Bộ xử lý không thể phản hồi người yêu cầu. Điều này có thể xảy ra, "
                    + "ví dụ, nếu kết nối hết thời gian chờ hoặc nếu Life được khởi động lại trước khi phản hồi Yêu cầu HTTP.")
            .build();

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(STATUS_CODE);
        properties.add(HTTP_CONTEXT_MAP);
        properties.add(ATTRIBUTES_AS_HEADERS_REGEX);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        return relationships;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .description("Specifies the value to send for the '" + propertyDescriptorName + "' HTTP Header")
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .build();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final StopWatch stopWatch = new StopWatch(true);

        final String contextIdentifier = flowFile.getAttribute(HTTPUtils.HTTP_CONTEXT_ID);
        if (contextIdentifier == null) {
            getLogger().warn("Failed to respond to HTTP request for {} because FlowFile did not have an '{}' attribute", flowFile, HTTPUtils.HTTP_CONTEXT_ID);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final String statusCodeValue = context.getProperty(STATUS_CODE).evaluateAttributeExpressions(flowFile).getValue();
        if (!isNumber(statusCodeValue)) {
            getLogger().error("Failed to respond to HTTP request for {} because status code was '{}', which is not a valid number", flowFile, statusCodeValue);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final HttpContextMap contextMap = context.getProperty(HTTP_CONTEXT_MAP).asControllerService(HttpContextMap.class);
        final HttpServletResponse response = contextMap.getResponse(contextIdentifier);
        if (response == null) {
            getLogger().error("Failed to respond to HTTP request for {} because FlowFile had an '{}' attribute of {} but could not find an HTTP Response Object for this identifier",
                    flowFile, HTTPUtils.HTTP_CONTEXT_ID, contextIdentifier);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final int statusCode = Integer.parseInt(statusCodeValue);
        response.setStatus(statusCode);

        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            final PropertyDescriptor descriptor = entry.getKey();
            if (descriptor.isDynamic()) {
                final String headerName = descriptor.getName();
                final String headerValue = context.getProperty(descriptor).evaluateAttributeExpressions(flowFile).getValue();

                if (!headerValue.trim().isEmpty()) {
                    response.setHeader(headerName, headerValue);
                }
            }
        }

        final String attributeHeaderRegex = context.getProperty(ATTRIBUTES_AS_HEADERS_REGEX).getValue();
        if (attributeHeaderRegex != null) {
            final Pattern pattern = Pattern.compile(attributeHeaderRegex);

            final Map<String, String> attributes = flowFile.getAttributes();
            for (final Map.Entry<String, String> entry : attributes.entrySet()) {
                final String key = entry.getKey();
                if (pattern.matcher(key).matches()) {
                    if (!entry.getValue().trim().isEmpty()){
                        response.setHeader(entry.getKey(), entry.getValue());
                    }
                }
            }
        }

        try {
            session.exportTo(flowFile, response.getOutputStream());
            response.flushBuffer();
        } catch (final ProcessException e) {
            getLogger().error("Failed to respond to HTTP request for {}", flowFile, e);
            try {
                contextMap.complete(contextIdentifier);
            } catch (final RuntimeException ce) {
                getLogger().error("Failed to complete HTTP Transaction for {}", flowFile, ce);
            }
            session.transfer(flowFile, REL_FAILURE);
            return;
        } catch (final Exception e) {
            getLogger().error("Failed to respond to HTTP request for {}", flowFile, e);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        try {
            contextMap.complete(contextIdentifier);
        } catch (final RuntimeException ce) {
            getLogger().error("Failed to complete HTTP Transaction for {}", flowFile, ce);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        session.getProvenanceReporter().send(flowFile, HTTPUtils.getURI(flowFile.getAttributes()), stopWatch.getElapsed(TimeUnit.MILLISECONDS));
        getLogger().info("Successfully responded to HTTP Request for {} with status code {}", flowFile, statusCode);
        session.transfer(flowFile, REL_SUCCESS);
    }

    private static boolean isNumber(final String value) {
        if (value.length() == 0) {
            return false;
        }

        for (int i = 0; i < value.length(); i++) {
            if (!Character.isDigit(value.charAt(i))) {
                return false;
            }
        }

        return true;
    }
}
