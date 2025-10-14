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
package org.apache.nifi.record.sink.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.kafka.shared.component.KafkaClientComponent;
import org.apache.nifi.kafka.shared.property.provider.KafkaPropertyProvider;
import org.apache.nifi.kafka.shared.property.provider.StandardKafkaPropertyProvider;
import org.apache.nifi.kafka.shared.validation.KafkaClientCustomValidationFunction;
import org.apache.nifi.kafka.shared.validation.DynamicPropertyValidator;
import org.apache.nifi.kafka.shared.validation.KafkaDeprecationValidator;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.sink.RecordSinkService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.stream.io.ByteCountingOutputStream;
import org.apache.nifi.stream.io.exception.TokenTooLargeException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Tags({"kafka", "record", "sink"})
@CapabilityDescription("Cung cấp dịch vụ ghi các record vào một topic Kafka 2.6+.")
@DynamicProperty(
    name = "Tên property cấu hình Kafka",
    value = "Giá trị của property Kafka tương ứng",
    description = "Các property này sẽ được thêm vào cấu hình Kafka sau khi tải bất kỳ cấu hình nào đã cung cấp. "
                + "Nếu một dynamic property trùng với property đã được thiết lập trước đó, giá trị mới sẽ bị bỏ qua và log cảnh báo. "
                + "Danh sách các property Kafka có thể tham khảo tại: http://kafka.apache.org/documentation.html#configuration.",
    expressionLanguageScope = ExpressionLanguageScope.VARIABLE_REGISTRY
)
public class KafkaRecordSink_2_6 extends AbstractControllerService implements KafkaClientComponent, RecordSinkService {

    static final AllowableValue DELIVERY_REPLICATED = new AllowableValue("all", "Guarantee Replicated Delivery",
            "Các record được coi là 'gửi thất bại' trừ khi message được replicate đến đủ số node Kafka theo cấu hình topic.");
    static final AllowableValue DELIVERY_ONE_NODE = new AllowableValue("1", "Guarantee Single Node Delivery",
            "Các record được coi là 'gửi thành công' nếu message được nhận bởi một node Kafka duy nhất, "
            + "không quan tâm việc replicate. Nhanh hơn <Guarantee Replicated Delivery> nhưng có thể mất dữ liệu nếu node Kafka gặp sự cố.");
    static final AllowableValue DELIVERY_BEST_EFFORT = new AllowableValue("0", "Best Effort",
            "Các record được coi là 'gửi thành công' ngay sau khi ghi nội dung vào node Kafka mà không đợi phản hồi. "
            + "Hiệu năng tốt nhất nhưng có thể dẫn đến mất dữ liệu.");

    static final PropertyDescriptor TOPIC = new PropertyDescriptor.Builder()
            .name("topic")
            .displayName("Topic Name")
            .description("Tên topic Kafka để publish đến.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor DELIVERY_GUARANTEE = new PropertyDescriptor.Builder()
            .name("acks")
            .displayName("Delivery Guarantee")
            .description("Xác định yêu cầu đảm bảo message được gửi đến Kafka. Tương ứng với property 'acks' của Kafka.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues(DELIVERY_BEST_EFFORT, DELIVERY_ONE_NODE, DELIVERY_REPLICATED)
            .defaultValue(DELIVERY_BEST_EFFORT.getValue())
            .build();

    static final PropertyDescriptor METADATA_WAIT_TIME = new PropertyDescriptor.Builder()
            .name("max.block.ms")
            .displayName("Max Metadata Wait Time")
            .description("Thời gian publisher sẽ đợi để lấy metadata hoặc đợi buffer flush khi gọi 'send' trước khi thất bại. "
                    + "Tương ứng với property 'max.block.ms' của Kafka.")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("5 sec")
            .build();

    static final PropertyDescriptor ACK_WAIT_TIME = new PropertyDescriptor.Builder()
            .name("ack.wait.time")
            .displayName("Acknowledgment Wait Time")
            .description("Thời gian đợi phản hồi từ Kafka sau khi gửi message. Nếu Kafka không ack trong thời gian này, FlowFile sẽ bị route sang 'failure'.")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .defaultValue("5 secs")
            .build();

    static final PropertyDescriptor MAX_REQUEST_SIZE = new PropertyDescriptor.Builder()
            .name("max.request.size")
            .displayName("Max Request Size")
            .description("Kích thước tối đa của một request tính bằng bytes. Tương ứng với property 'max.request.size' của Kafka, mặc định 1 MB (1048576).")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("1 MB")
            .build();

    static final PropertyDescriptor COMPRESSION_CODEC = new PropertyDescriptor.Builder()
            .name("compression.type")
            .displayName("Compression Type")
            .description("Chỉ định codec nén cho tất cả dữ liệu được producer tạo ra.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues("none", "gzip", "snappy", "lz4")
            .defaultValue("none")
            .build();

    static final PropertyDescriptor MESSAGE_HEADER_ENCODING = new PropertyDescriptor.Builder()
            .name("message-header-encoding")
            .displayName("Message Header Encoding")
            .description("Đối với bất kỳ attribute nào được gửi làm header message, thuộc tính này chỉ định Character Encoding dùng để serialize header.")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue("UTF-8")
            .required(false)
            .build();

    private List<PropertyDescriptor> properties;
    private volatile RecordSetWriterFactory writerFactory;
    private volatile int maxMessageSize;
    private volatile long maxAckWaitMillis;
    private volatile String topic;
    private volatile Producer<byte[], byte[]> producer;

    @Override
    protected void init(final ControllerServiceInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(BOOTSTRAP_SERVERS);
        properties.add(TOPIC);
        properties.add(RecordSinkService.RECORD_WRITER_FACTORY);
        properties.add(DELIVERY_GUARANTEE);
        properties.add(MESSAGE_HEADER_ENCODING);
        properties.add(SECURITY_PROTOCOL);
        properties.add(KERBEROS_CREDENTIALS_SERVICE);
        properties.add(SELF_CONTAINED_KERBEROS_USER_SERVICE);
        properties.add(KERBEROS_SERVICE_NAME);
        properties.add(SSL_CONTEXT_SERVICE);
        properties.add(MAX_REQUEST_SIZE);
        properties.add(ACK_WAIT_TIME);
        properties.add(METADATA_WAIT_TIME);
        properties.add(COMPRESSION_CODEC);
        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .description("Specifies the value for '" + propertyDescriptorName + "' Kafka Configuration.")
                .name(propertyDescriptorName)
                .addValidator(new DynamicPropertyValidator(ProducerConfig.class))
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        KafkaDeprecationValidator.validate(getClass(), getIdentifier(), validationContext);
        return new KafkaClientCustomValidationFunction().apply(validationContext);
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        topic = context.getProperty(TOPIC).evaluateAttributeExpressions().getValue();
        writerFactory = context.getProperty(RecordSinkService.RECORD_WRITER_FACTORY).asControllerService(RecordSetWriterFactory.class);
        maxMessageSize = context.getProperty(MAX_REQUEST_SIZE).asDataSize(DataUnit.B).intValue();
        maxAckWaitMillis = context.getProperty(ACK_WAIT_TIME).asTimePeriod(TimeUnit.MILLISECONDS);

        final KafkaPropertyProvider propertyProvider = new StandardKafkaPropertyProvider(ProducerConfig.class);
        final Map<String, Object> kafkaProperties = propertyProvider.getProperties(context);
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProperties.put("max.request.size", String.valueOf(maxMessageSize));

        try {
            producer = createProducer(kafkaProperties);
        } catch (Exception e) {
            getLogger().error("Could not create Kafka producer due to {}", e.getMessage(), e);
            throw new InitializationException(e);
        }
    }

    @Override
    public WriteResult sendData(final RecordSet recordSet, final Map<String, String> attributes, final boolean sendZeroResults) throws IOException {

        try {
            final RecordSchema writeSchema = getWriterFactory().getSchema(null, recordSet.getSchema());
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final ByteCountingOutputStream out = new ByteCountingOutputStream(baos);
            final Queue<Future<RecordMetadata>> ackQ = new LinkedList<>();
            int recordCount = 0;
            try (final RecordSetWriter writer = getWriterFactory().createWriter(getLogger(), writeSchema, out, attributes)) {
                Record record;
                while ((record = recordSet.next()) != null) {
                    baos.reset();
                    out.reset();
                    writer.write(record);
                    writer.flush();
                    recordCount++;
                    if (out.getBytesWritten() > maxMessageSize) {
                        throw new TokenTooLargeException("A record's size exceeds the maximum allowed message size of " + maxMessageSize + " bytes.");
                    }
                    sendMessage(topic, baos.toByteArray(), ackQ);
                }
                if (out.getBytesWritten() > maxMessageSize) {
                    throw new TokenTooLargeException("A record's size exceeds the maximum allowed message size of " + maxMessageSize + " bytes.");
                }

                attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                attributes.put("record.count", Integer.toString(recordCount));
            }

            if (recordCount == 0) {
                if (sendZeroResults) {
                    sendMessage(topic, new byte[0], ackQ);
                } else {
                    return WriteResult.EMPTY;
                }
            }

            acknowledgeTransmission(ackQ);

            return WriteResult.of(recordCount, attributes);
        } catch (IOException ioe) {
            throw ioe;
        } catch (Exception e) {
            throw new IOException("Failed to write metrics using record writer: " + e.getMessage(), e);
        }
    }

    private void sendMessage(String topic, byte[] payload, final Queue<Future<RecordMetadata>> ackQ) throws IOException {
        final ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, null, null, payload);
        // Add the Future to the queue
        ackQ.add(producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                throw new KafkaSendException(exception);
            }
        }));
    }

    private void acknowledgeTransmission(final Queue<Future<RecordMetadata>> ackQ) throws IOException, ExecutionException {
        try {
            Future<RecordMetadata> ack;
            while ((ack = ackQ.poll()) != null) {
                ack.get(maxAckWaitMillis, TimeUnit.MILLISECONDS);
            }
        } catch (KafkaSendException kse) {
            Throwable t = kse.getCause();
            if (t instanceof IOException) {
                throw (IOException) t;
            } else {
                throw new IOException(t);
            }
        } catch (final InterruptedException e) {
            getLogger().warn("Interrupted while waiting for an acknowledgement from Kafka");
            Thread.currentThread().interrupt();
        } catch (final TimeoutException e) {
            getLogger().warn("Timed out while waiting for an acknowledgement from Kafka");
        }
    }

    @OnDisabled
    public void stop() {
        if (producer != null) {
            producer.close(Duration.ofMillis(maxAckWaitMillis));
        }
    }

    // this getter is intended explicitly for testing purposes
    protected RecordSetWriterFactory getWriterFactory() {
        return this.writerFactory;
    }

    protected Producer<byte[], byte[]> createProducer(Map<String, Object> kafkaProperties) {
        return new KafkaProducer<>(kafkaProperties);
    }

    private static class KafkaSendException extends RuntimeException {
        KafkaSendException(Throwable cause) {
            super(cause);
        }
    }
}
