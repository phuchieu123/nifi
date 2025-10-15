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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fluenda.parcefone.event.CEFHandlingException;
import com.fluenda.parcefone.event.CommonEvent;
import com.fluenda.parcefone.event.MacAddress;
import com.fluenda.parcefone.parser.CEFParser;
import org.apache.bval.jsr.ApacheValidationProvider;
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
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.annotation.documentation.DeprecationNotice;

import javax.validation.Validation;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"logs", "cef", "attributes", "system", "event", "message"})
@CapabilityDescription("Phân tích cú pháp nội dung của một thông điệp định dạng CEF và thêm các thuộc tính vào FlowFile cho " +
        "các tiêu đề và phần mở rộng của các phần của thông điệp CEF.\n" +
        "Lưu ý: Bộ xử lý này mong đợi các thông điệp CEF KHÔNG có tiêu đề syslog (tức là bắt đầu từ \"CEF:0\"")
@WritesAttributes({@WritesAttribute(attribute = "cef.header.version", description = "Phiên bản của thông điệp CEF."),
    @WritesAttribute(attribute = "cef.header.deviceVendor", description = "Nhà cung cấp thiết bị của thông điệp CEF."),
    @WritesAttribute(attribute = "cef.header.deviceProduct", description = "Sản phẩm thiết bị của thông điệp CEF."),
    @WritesAttribute(attribute = "cef.header.deviceVersion", description = "Phiên bản thiết bị của thông điệp CEF."),
    @WritesAttribute(attribute = "cef.header.deviceEventClassId", description = "ID Lớp sự kiện thiết bị của thông điệp CEF."),
    @WritesAttribute(attribute = "cef.header.name", description = "Tên của thông điệp CEF."),
    @WritesAttribute(attribute = "cef.header.severity", description = "Mức độ nghiêm trọng của thông điệp CEF."),
    @WritesAttribute(attribute = "cef.extension.*", description = "Khóa và giá trị được tạo ra bởi việc phân tích cú pháp thông điệp.")})
@SeeAlso({ParseSyslog.class})
@DeprecationNotice(reason = "Thành phần này không được dùng nữa và sẽ bị xóa trong NiFi 2.x.")
public class ParseCEF extends AbstractProcessor {

    // Không nên có định dạng ngày nào khác ngoài các định dạng đã được quốc tế đồng ý...
    // flowfile-attributes sử dụng thời gian Java 8 để phân tích cú pháp dữ liệu (vì các đối tượng Date không có múi giờ)
    private final static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

    // vì một lý do nào đó Jackson dường như không thể sử dụng DateTieFormater
    // vì vậy chúng tôi sử dụng SimpleDateFormat để định dạng trong flowfile-content
    private final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");


    // thêm một đối tượng TZ để được sử dụng bởi quy trình flowfile-attribute
    private String tzId = null;

    // Thêm trình tuần tự hóa và trình ánh xạ
    private static final ObjectMapper mapper = new ObjectMapper();

    public static final String DESTINATION_CONTENT = "flowfile-content";
    public static final String DESTINATION_ATTRIBUTES = "flowfile-attribute";
    public static final PropertyDescriptor FIELDS_DESTINATION = new PropertyDescriptor.Builder()
        .name("FIELDS_DESTINATION")
        .displayName("Đích đến của các trường đã phân tích cú pháp")
        .description(
                "Chỉ định liệu kết quả của trình phân tích cú pháp CEF được ghi vào " +
                "nội dung FlowFile hay một thuộc tính FlowFile; nếu sử dụng " + DESTINATION_ATTRIBUTES +
                "thuộc tính, các trường sẽ được điền dưới dạng thuộc tính. " +
                "Nếu được đặt thành " + DESTINATION_CONTENT + ", trường mở rộng CEF sẽ được chuyển đổi thành " +
                "một đối tượng JSON phẳng.")
        .required(true)
        .allowableValues(DESTINATION_CONTENT, DESTINATION_ATTRIBUTES)
        .defaultValue(DESTINATION_CONTENT)
        .build();

    public static final PropertyDescriptor APPEND_RAW_MESSAGE_TO_JSON = new PropertyDescriptor.Builder()
            .name("APPEND_RAW_MESSAGE_TO_JSON")
            .displayName("Nối thông điệp thô vào JSON")
            .description("Khi sử dụng flowfile-content (tức là đầu ra JSON), hãy thêm thông điệp CEF gốc vào " +
                    "đối tượng JSON kết quả. Thông điệp gốc được thêm dưới dạng một chuỗi vào _raw.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .required(true)
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor INCLUDE_CUSTOM_EXTENSIONS = new PropertyDescriptor.Builder()
            .name("INCLUDE_CUSTOM_EXTENSIONS")
            .displayName("Bao gồm các phần mở rộng tùy chỉnh")
            .description("Nếu được đặt thành true, các phần mở rộng tùy chỉnh (không được chỉ định trong thông số kỹ thuật CEF) sẽ được "
                    + "bao gồm trong dữ liệu/thuộc tính được tạo ra.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .required(true)
            .defaultValue("false")
            .allowableValues("true", "false")
            .build();

    public static final PropertyDescriptor ACCEPT_EMPTY_EXTENSIONS = new PropertyDescriptor.Builder()
            .name("ACCEPT_EMPTY_EXTENSIONS")
            .displayName("Chấp nhận các phần mở rộng trống")
            .description("Nếu được đặt thành true, các phần mở rộng trống sẽ được chấp nhận và sẽ được liên kết với một giá trị null.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .required(true)
            .defaultValue("false")
            .allowableValues("true", "false")
            .build();

    public static final PropertyDescriptor VALIDATE_DATA = new PropertyDescriptor.Builder()
            .name("VALIDATE_DATA")
            .displayName("Xác thực sự kiện CEF")
            .description("Nếu được đặt thành true, sự kiện sẽ được xác thực theo tiêu chuẩn CEF (bản sửa đổi 23). Nếu sự kiện không hợp lệ, "
                    + "FlowFile sẽ được chuyển đến mối quan hệ thất bại. Nếu thuộc tính này được đặt thành false, sự kiện sẽ được xử lý "
                    + "mà không cần xác thực dữ liệu.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .required(true)
            .defaultValue("true")
            .allowableValues("true", "false")
            .build();

    public static final String UTC = "UTC";
    public static final String LOCAL_TZ = "Múi giờ địa phương (Mặc định hệ thống)";
    public static final PropertyDescriptor TIME_REPRESENTATION = new PropertyDescriptor.Builder()
        .name("TIME_REPRESENTATION")
        .displayName("Múi giờ")
        .description("Múi giờ sẽ được sử dụng khi biểu diễn các trường ngày. UTC sẽ chuyển đổi tất cả " +
                "các ngày sang UTC, trong khi Múi giờ địa phương sẽ chuyển đổi chúng sang múi giờ được sử dụng bởi NiFi.")
        .allowableValues(UTC, LOCAL_TZ)
        .required(true)
        .defaultValue(LOCAL_TZ)
        .build();

    public static final PropertyDescriptor DATETIME_REPRESENTATION = new PropertyDescriptor.Builder()
        .name("DATETIME_REPRESENTATION")
        .displayName("Miền địa phương DateTime")
        .description("Biểu diễn IETF BCP 47 của Miền địa phương sẽ được sử dụng khi phân tích cú pháp các trường ngày " +
                "với tên tháng dài hoặc ngắn (ví dụ: may <en-US> so với mai. <fr-FR>. Giá trị mặc định" +
                "thường là an toàn. Chỉ thay đổi nếu gặp sự cố khi phân tích cú pháp các thông điệp CEF")
        .required(true)
        .addValidator(new ValidateLocale())
        .defaultValue("en-US")
        .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("thất bại")
        .description("Bất kỳ FlowFile nào không thể được phân tích cú pháp dưới dạng thông điệp CEF sẽ được chuyển đến Mối quan hệ này mà không cần thêm bất kỳ thuộc tính nào")
        .build();
    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("thành công")
        .description("Bất kỳ FlowFile nào được phân tích cú pháp thành công dưới dạng thông điệp CEF sẽ được chuyển đến Mối quan hệ này.")
        .build();
    // Create a Bean validator to be shared by the parser instances.
    final javax.validation.Validator validator = Validation.byProvider(ApacheValidationProvider.class).configure().buildValidatorFactory().getValidator();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor>properties = new ArrayList<>();
        properties.add(FIELDS_DESTINATION);
        properties.add(APPEND_RAW_MESSAGE_TO_JSON);
        properties.add(INCLUDE_CUSTOM_EXTENSIONS);
        properties.add(ACCEPT_EMPTY_EXTENSIONS);
        properties.add(VALIDATE_DATA);
        properties.add(TIME_REPRESENTATION);
        properties.add(DATETIME_REPRESENTATION);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_FAILURE);
        relationships.add(REL_SUCCESS);
        return relationships;
    }

    @OnScheduled
    public void OnScheduled(final ProcessContext context) {

        // Configure jackson mapper before spawning onTriggers
        final SimpleModule module = new SimpleModule()
                                        .addSerializer(MacAddress.class, new MacAddressToStringSerializer());
        mapper.registerModule(module);
        mapper.setDateFormat(this.simpleDateFormat);

        switch (context.getProperty(TIME_REPRESENTATION).getValue()) {
            case LOCAL_TZ:
                // set the mapper TZ to local TZ
                mapper.setTimeZone(TimeZone.getDefault());
                tzId = TimeZone.getDefault().getID();
                break;
            case UTC:
                // set the mapper TZ to local TZ
                mapper.setTimeZone(TimeZone.getTimeZone(UTC));
                tzId = UTC;
                break;
        }

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final CEFParser parser = new CEFParser(validator);

        final byte[] buffer = new byte[(int) flowFile.getSize()];
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                StreamUtils.fillBuffer(in, buffer);
            }
        });

        CommonEvent event;

        try {
            // parcefoneLocale defaults to en_US, so this should not fail. But we force failure in case the custom
            // validator failed to identify an invalid Locale
            final Locale parcefoneLocale = Locale.forLanguageTag(context.getProperty(DATETIME_REPRESENTATION).getValue());
            final boolean validateData = context.getProperty(VALIDATE_DATA).asBoolean();
            final boolean acceptEmptyExtensions = context.getProperty(ACCEPT_EMPTY_EXTENSIONS).asBoolean();
            event = parser.parse(buffer, validateData, acceptEmptyExtensions, parcefoneLocale);

        } catch (Exception e) {
            // This should never trigger but adding in here as a fencing mechanism to
            // address possible ParCEFone bugs.
            getLogger().error("CEF Parsing Failed: {}", flowFile, e);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }


        // ParCEFone returns null every time it cannot parse an
        // event, so we test
        if (event==null) {
            getLogger().error("Failed to parse {} as a CEF message: it does not conform to the CEF standard; routing to failure", flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }


        try {
            final String destination = context.getProperty(FIELDS_DESTINATION).getValue();
            final boolean includeCustomExtensions = context.getProperty(INCLUDE_CUSTOM_EXTENSIONS).asBoolean();

            switch (destination) {
                case DESTINATION_ATTRIBUTES:

                    final Map<String, String> attributes = new HashMap<>();

                    // Process KVs of the Header field
                    for (Map.Entry<String, Object> entry : event.getHeader().entrySet()) {
                        attributes.put("cef.header."+entry.getKey(), prettyResult(entry.getValue(), tzId));
                    }

                    // Process KVs composing the Extension field
                    for (Map.Entry<String, Object> entry : event.getExtension(true, includeCustomExtensions).entrySet()) {
                    attributes.put("cef.extension." + entry.getKey(), prettyResult(entry.getValue(), tzId));

                    flowFile = session.putAllAttributes(flowFile, attributes);
                    }
                    break;

                case DESTINATION_CONTENT:

                    ObjectNode results = mapper.createObjectNode();

                    // Add two JSON objects containing one CEF field each
                    results.set("header", mapper.valueToTree(event.getHeader()));
                    results.set("extension", mapper.valueToTree(event.getExtension(true, includeCustomExtensions)));

                    // Add the original content to original CEF content
                    // to the resulting JSON
                    if (context.getProperty(APPEND_RAW_MESSAGE_TO_JSON).asBoolean()) {
                        results.set("_raw", mapper.valueToTree(new String(buffer)));
                    }

                    flowFile = session.write(flowFile, new OutputStreamCallback() {
                        @Override
                        public void process(OutputStream out) throws IOException {
                            try (OutputStream outputStream = new BufferedOutputStream(out)) {
                                outputStream.write(mapper.writeValueAsBytes(results));
                            }
                        }
                    });

                    // Adjust the FlowFile mime.type attribute
                    flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/json");

                    // Update the provenance for good measure
                    session.getProvenanceReporter().modifyContent(flowFile, "Replaced content with parsed CEF fields and values");
                    break;
            }

            // whatever the parsing stratgy, ready to transfer to success and commit
            session.transfer(flowFile, REL_SUCCESS);
        } catch (CEFHandlingException e) {
            // The flowfile has failed parsing & validation, routing to failure and committing
            getLogger().error("Reading CEF Event Failed: {}", flowFile, e);
            // Create a provenance event recording the routing to failure
            session.getProvenanceReporter().route(flowFile, REL_FAILURE);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private String prettyResult(Object entryValue, String tzID) {

        if (entryValue instanceof InetAddress ) {
            return ((InetAddress) entryValue).getHostAddress();
        } else if (entryValue instanceof Date) {
            ZonedDateTime zdt = ZonedDateTime.from(((Date) entryValue).toInstant().atZone(ZoneId.of(tzID)));
            return(String.valueOf(zdt.format(dateTimeFormatter)));
        } else {
            return String.valueOf(entryValue);
        }
    }


    // Serialize MacAddress as plain string
    private class MacAddressToStringSerializer extends JsonSerializer<MacAddress> {

        @Override
        public void serialize(MacAddress macAddress,
                              JsonGenerator jsonGenerator,
                              SerializerProvider serializerProvider)
                throws IOException {
            jsonGenerator.writeObject(macAddress.toString());
        }
    }


    protected static class ValidateLocale implements Validator {
        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            if (null == input || input.isEmpty()) {
                return new ValidationResult.Builder().subject(subject).input(input).valid(false)
                        .explanation(subject + " cannot be empty").build();
            }

            final Locale testLocale = Locale.forLanguageTag(input);

            // Check if the provided Locale is valid by checking against the empty locale string
            if ("".equals(testLocale.toString())) {
                // Locale matches the "null" locale so it is treated as invalid
                return new ValidationResult.Builder().subject(subject).input(input).valid(false)
                        .explanation(input + " is not a valid locale format.").build();
            } else {
                return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
            }

        }
    }
}
