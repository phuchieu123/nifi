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
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.syslog.attributes.SyslogAttributes;
import org.apache.nifi.syslog.events.SyslogEvent;
import org.apache.nifi.syslog.parsers.SyslogParser;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"logs", "syslog", "attributes", "system", "event", "message"})
@CapabilityDescription("Cố gắng phân tích cú pháp nội dung của một tin nhắn Syslog theo các định dạng RFC5424 và RFC3164 " +
        "và thêm các thuộc tính vào FlowFile cho từng phần của tin nhắn Syslog." +
        "Lưu ý: Hãy nhớ rằng RFC3164 chỉ mang tính thông tin và có rất nhiều cách triển khai khác nhau trong" +
        " thực tế. Nếu việc phân tích cú pháp tin nhắn không thành công, hãy cân nhắc sử dụng RFC5424 hoặc sử dụng một bộ xử lý phân tích cú pháp chung như " +
        "ExtractGrok.")
@WritesAttributes({@WritesAttribute(attribute = "syslog.priority", description = "Mức độ ưu tiên của tin nhắn Syslog."),
    @WritesAttribute(attribute = "syslog.severity", description = "Mức độ nghiêm trọng của tin nhắn Syslog được suy ra từ mức độ ưu tiên."),
    @WritesAttribute(attribute = "syslog.facility", description = "Cơ sở của tin nhắn Syslog được suy ra từ mức độ ưu tiên."),
    @WritesAttribute(attribute = "syslog.version", description = "Phiên bản tùy chọn từ tin nhắn Syslog."),
    @WritesAttribute(attribute = "syslog.timestamp", description = "Dấu thời gian của tin nhắn Syslog."),
    @WritesAttribute(attribute = "syslog.hostname", description = "Tên máy chủ hoặc địa chỉ IP của tin nhắn Syslog."),
    @WritesAttribute(attribute = "syslog.sender", description = "Tên máy chủ của máy chủ Syslog đã gửi tin nhắn."),
    @WritesAttribute(attribute = "syslog.body", description = "Nội dung của tin nhắn Syslog, mọi thứ sau tên máy chủ.")})
@SeeAlso({ListenSyslog.class, PutSyslog.class})
public class ParseSyslog extends AbstractProcessor {

    public static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
        .name("Bộ ký tự")
        .description("Chỉ định bộ ký tự của các tin nhắn Syslog")
        .required(true)
        .defaultValue("UTF-8")
        .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
        .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("thất bại")
        .description("Bất kỳ FlowFile nào không thể phân tích cú pháp dưới dạng tin nhắn Syslog sẽ được chuyển đến Relationship này mà không cần thêm bất kỳ thuộc tính nào")
        .build();
    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("thành công")
        .description("Bất kỳ FlowFile nào được phân tích cú pháp thành công dưới dạng tin nhắn Syslog sẽ được chuyển đến Relationship này.")
        .build();
    private SyslogParser parser;


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(1);
        properties.add(CHARSET);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_FAILURE);
        relationships.add(REL_SUCCESS);
        return relationships;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String charsetName = context.getProperty(CHARSET).getValue();

        // If the parser already exists and uses the same charset, it does not need to be re-initialized
        if (parser == null || !parser.getCharsetName().equals(charsetName)) {
            parser = new SyslogParser(Charset.forName(charsetName));
        }

        final byte[] buffer = new byte[(int) flowFile.getSize()];
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                StreamUtils.fillBuffer(in, buffer);
            }
        });

        final SyslogEvent event;
        try {
            event = parser.parseEvent(buffer, null);
        } catch (final ProcessException pe) {
            getLogger().error("Failed to parse {} as a Syslog message", flowFile, pe);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        if (event == null || !event.isValid()) {
            getLogger().error("Failed to parse {} as a Syslog message: it does not conform to any of the RFC formats supported; routing to failure", flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final Map<String, String> attributes = new HashMap<>(8);
        attributes.put(SyslogAttributes.SYSLOG_PRIORITY.key(), event.getPriority());
        attributes.put(SyslogAttributes.SYSLOG_SEVERITY.key(), event.getSeverity());
        attributes.put(SyslogAttributes.SYSLOG_FACILITY.key(), event.getFacility());
        attributes.put(SyslogAttributes.SYSLOG_VERSION.key(), event.getVersion());
        attributes.put(SyslogAttributes.SYSLOG_TIMESTAMP.key(), event.getTimeStamp());
        attributes.put(SyslogAttributes.SYSLOG_HOSTNAME.key(), event.getHostName());
        attributes.put(SyslogAttributes.SYSLOG_BODY.key(), event.getMsgBody());

        flowFile = session.putAllAttributes(flowFile, attributes);
        session.transfer(flowFile, REL_SUCCESS);
    }

}
