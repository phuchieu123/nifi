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

package org.apache.nifi.reporting;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.reporting.s2s.SiteToSiteUtils;
import org.apache.nifi.reporting.util.provenance.ProvenanceEventConsumer;

@Tags({"provenance", "lineage", "tracking", "site", "site to site"})
@CapabilityDescription("Công bố các sự kiện Provenance bằng giao thức Site-to-Site.")
@Stateful(scopes = Scope.LOCAL, description = "Lưu trữ ID sự kiện cuối cùng của Tác vụ Báo cáo để khi khởi động lại, tác vụ biết nó đã dừng ở đâu.")
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.EXPORT_NIFI_DETAILS,
                        explanation = "Cung cấp cho người vận hành khả năng gửi các chi tiết nhạy cảm có trong các sự kiện Provenance đến bất kỳ hệ thống bên ngoài nào.")
        }
)
public class SiteToSiteProvenanceReportingTask extends AbstractSiteToSiteReportingTask {

    static final AllowableValue BEGINNING_OF_STREAM = new AllowableValue("beginning-of-stream", "Bắt đầu Luồng",
            "Bắt đầu đọc các Sự kiện Provenance từ đầu luồng (sự kiện cũ nhất trước tiên)");
    static final AllowableValue END_OF_STREAM = new AllowableValue("end-of-stream", "Cuối Luồng",
            "Bắt đầu đọc các Sự kiện Provenance từ cuối luồng, bỏ qua các sự kiện cũ");

    static final PropertyDescriptor FILTER_EVENT_TYPE = new PropertyDescriptor.Builder()
            .name("s2s-prov-task-event-filter")
            .displayName("Loại Sự kiện cần Bao gồm")
            .description("Danh sách các loại sự kiện được phân tách bằng dấu phẩy sẽ được sử dụng để lọc các sự kiện provenance được gửi bởi tác vụ báo cáo. "
                    + "Các loại sự kiện có sẵn là " + Arrays.deepToString(ProvenanceEventType.values()) + ". Nếu không có bộ lọc nào được đặt, tất cả các sự kiện sẽ được gửi. Nếu "
                    + "nhiều bộ lọc được đặt, các bộ lọc sẽ được cộng dồn.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor FILTER_EVENT_TYPE_EXCLUDE = new PropertyDescriptor.Builder()
            .name("s2s-prov-task-event-filter-exclude")
            .displayName("Loại Sự kiện cần Loại trừ")
            .description("Danh sách các loại sự kiện được phân tách bằng dấu phẩy sẽ được sử dụng để loại trừ các sự kiện provenance được gửi bởi tác vụ báo cáo. "
                    + "Các loại sự kiện có sẵn là " + Arrays.deepToString(ProvenanceEventType.values()) + ". Nếu không có bộ lọc nào được đặt, tất cả các sự kiện sẽ được gửi. Nếu "
                    + "nhi-ều bộ lọc được đặt, các bộ lọc sẽ được cộng dồn. Nếu một loại sự kiện được bao gồm trong Loại Sự kiện cần Bao gồm và bị loại trừ ở đây, thì "
                    + "việc loại trừ sẽ được ưu tiên và sự kiện đó sẽ không được gửi.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor FILTER_COMPONENT_TYPE = new PropertyDescriptor.Builder()
            .name("s2s-prov-task-type-filter")
            .displayName("Loại Thành phần cần Bao gồm")
            .description("Biểu thức chính quy để lọc các sự kiện provenance dựa trên loại thành phần. Chỉ những sự kiện khớp với biểu thức chính quy "
                    + "mới được gửi. Nếu không có bộ lọc nào được đặt, tất cả các sự kiện sẽ được gửi. Nếu nhiều bộ lọc được đặt, các bộ lọc sẽ được cộng dồn.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    static final PropertyDescriptor FILTER_COMPONENT_TYPE_EXCLUDE = new PropertyDescriptor.Builder()
            .name("s2s-prov-task-type-filter-exclude")
            .displayName("Loại Thành phần cần Loại trừ")
            .description("Biểu thức chính quy để loại trừ các sự kiện provenance dựa trên loại thành phần. Các sự kiện khớp với biểu thức chính quy "
                    + "sẽ không được gửi. Nếu không có bộ lọc nào được đặt, tất cả các sự kiện sẽ được gửi. Nếu nhiều bộ lọc được đặt, các bộ lọc sẽ được cộng dồn. "
                    + "Nếu một loại thành phần được bao gồm trong Loại Thành phần cần Bao gồm và bị loại trừ ở đây, thì việc loại trừ sẽ được ưu tiên và sự kiện đó sẽ không được gửi.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    static final PropertyDescriptor FILTER_COMPONENT_ID = new PropertyDescriptor.Builder()
            .name("s2s-prov-task-id-filter")
            .displayName("ID Thành phần cần Bao gồm")
            .description("Danh sách các UUID của thành phần được phân tách bằng dấu phẩy sẽ được sử dụng để lọc các sự kiện provenance được gửi bởi tác vụ báo cáo. Nếu không có "
                    + "bộ lọc nào được đặt, tất cả các sự kiện sẽ được gửi. Nếu nhiều bộ lọc được đặt, các bộ lọc sẽ được cộng dồn.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor FILTER_COMPONENT_ID_EXCLUDE = new PropertyDescriptor.Builder()
            .name("s2s-prov-task-id-filter-exclude")
            .displayName("ID Thành phần cần Loại trừ")
            .description("Danh sách các UUID của thành phần được phân tách bằng dấu phẩy sẽ được sử dụng để loại trừ các sự kiện provenance được gửi bởi tác vụ báo cáo. Nếu không có "
                    + "bộ lọc nào được đặt, tất cả các sự kiện sẽ được gửi. Nếu nhiều bộ lọc được đặt, các bộ lọc sẽ được cộng dồn. Nếu một UUID của thành phần được bao gồm trong "
                    + "ID Thành phần cần Bao gồm và bị loại trừ ở đây, thì việc loại trừ sẽ được ưu tiên và sự kiện đó sẽ không được gửi.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor FILTER_COMPONENT_NAME = new PropertyDescriptor.Builder()
            .name("s2s-prov-task-name-filter")
            .displayName("Tên Thành phần cần Bao gồm")
            .description("Biểu thức chính quy để lọc các sự kiện provenance dựa trên tên thành phần. Chỉ những sự kiện khớp với biểu thức chính quy "
                    + "mới được gửi. Nếu không có bộ lọc nào được đặt, tất cả các sự kiện sẽ được gửi. Nếu nhiều bộ lọc được đặt, các bộ lọc sẽ được cộng dồn.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    static final PropertyDescriptor FILTER_COMPONENT_NAME_EXCLUDE = new PropertyDescriptor.Builder()
            .name("s2s-prov-task-name-filter-exclude")
            .displayName("Tên Thành phần cần Loại trừ")
            .description("Biểu thức chính quy để loại trừ các sự kiện provenance dựa trên tên thành phần. Các sự kiện khớp với biểu thức chính quy "
                    + "sẽ không được gửi. Nếu không có bộ lọc nào được đặt, tất cả các sự kiện sẽ được gửi. Nếu nhiều bộ lọc được đặt, các bộ lọc sẽ được cộng dồn. "
                    + "Nếu một tên thành phần được bao gồm trong Tên Thành phần cần Bao gồm và bị loại trừ ở đây, thì việc loại trừ sẽ được ưu tiên và sự kiện đó sẽ không được gửi.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    static final PropertyDescriptor START_POSITION = new PropertyDescriptor.Builder()
            .name("start-position")
            .displayName("Vị trí Bắt đầu")
            .description("Nếu Tác vụ Báo cáo chưa từng được chạy, hoặc nếu trạng thái của nó đã được người dùng đặt lại, chỉ định nơi trong luồng Sự kiện Provenance mà Tác vụ Báo cáo nên bắt đầu")
            .allowableValues(BEGINNING_OF_STREAM, END_OF_STREAM)
            .defaultValue(BEGINNING_OF_STREAM.getValue())
            .required(true)
            .build();
    private volatile ProvenanceEventConsumer consumer;

    public SiteToSiteProvenanceReportingTask() throws IOException {
        final InputStream schema = getClass().getClassLoader().getResourceAsStream("schema-provenance.avsc");
        recordSchema = AvroTypeUtil.createSchema(new Schema.Parser().parse(schema));
    }

    @OnScheduled
    public void onScheduled(final ConfigurationContext context) throws IOException {
        consumer = new ProvenanceEventConsumer();
        consumer.setStartPositionValue(context.getProperty(START_POSITION).getValue());
        consumer.setBatchSize(context.getProperty(SiteToSiteUtils.BATCH_SIZE).asInteger());
        consumer.setLogger(getLogger());

        // initialize component type filtering
        consumer.setComponentTypeRegex(context.getProperty(FILTER_COMPONENT_TYPE).evaluateAttributeExpressions().getValue());
        consumer.setComponentTypeRegexExclude(context.getProperty(FILTER_COMPONENT_TYPE_EXCLUDE).evaluateAttributeExpressions().getValue());
        consumer.setComponentNameRegex(context.getProperty(FILTER_COMPONENT_NAME).evaluateAttributeExpressions().getValue());
        consumer.setComponentNameRegexExclude(context.getProperty(FILTER_COMPONENT_NAME_EXCLUDE).evaluateAttributeExpressions().getValue());

        final String[] targetEventTypes = StringUtils.stripAll(StringUtils.split(context.getProperty(FILTER_EVENT_TYPE).evaluateAttributeExpressions().getValue(), ','));
        if(targetEventTypes != null) {
            for(String type : targetEventTypes) {
                try {
                    consumer.addTargetEventType(ProvenanceEventType.valueOf(type));
                } catch (Exception e) {
                    getLogger().warn(type + " is not a correct event type, removed from the filtering.");
                }
            }
        }

        final String[] targetEventTypesExclude = StringUtils.stripAll(StringUtils.split(context.getProperty(FILTER_EVENT_TYPE_EXCLUDE).evaluateAttributeExpressions().getValue(), ','));
        if(targetEventTypesExclude != null) {
            for(String type : targetEventTypesExclude) {
                try {
                    consumer.addTargetEventTypeExclude(ProvenanceEventType.valueOf(type));
                } catch (Exception e) {
                    getLogger().warn(type + " is not a correct event type, removed from the exclude filtering.");
                }
            }
        }

        // initialize component ID filtering
        final String[] targetComponentIds = StringUtils.stripAll(StringUtils.split(context.getProperty(FILTER_COMPONENT_ID).evaluateAttributeExpressions().getValue(), ','));
        if(targetComponentIds != null) {
            consumer.addTargetComponentId(targetComponentIds);
        }

        final String[] targetComponentIdsExclude = StringUtils.stripAll(StringUtils.split(context.getProperty(FILTER_COMPONENT_ID_EXCLUDE).evaluateAttributeExpressions().getValue(), ','));
        if(targetComponentIdsExclude != null) {
            consumer.addTargetComponentIdExclude(targetComponentIdsExclude);
        }

        consumer.setScheduled(true);
    }

    @OnUnscheduled
    public void onUnscheduled() {
        if (consumer != null) {
            consumer.setScheduled(false);
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(SiteToSiteUtils.PLATFORM);
        properties.add(FILTER_EVENT_TYPE);
        properties.add(FILTER_EVENT_TYPE_EXCLUDE);
        properties.add(FILTER_COMPONENT_TYPE);
        properties.add(FILTER_COMPONENT_TYPE_EXCLUDE);
        properties.add(FILTER_COMPONENT_ID);
        properties.add(FILTER_COMPONENT_ID_EXCLUDE);
        properties.add(FILTER_COMPONENT_NAME);
        properties.add(FILTER_COMPONENT_NAME_EXCLUDE);
        properties.add(START_POSITION);
        return properties;
    }

    @Override
    public void onTrigger(final ReportingContext context) {
        final boolean isClustered = context.isClustered();
        final String nodeId = context.getClusterNodeIdentifier();
        if (nodeId == null && isClustered) {
            getLogger().debug("This instance of Life is configured for clustering, but the Cluster Node Identifier is not yet available. "
                + "Will wait for Node Identifier to be established.");
            return;
        }

        final ProcessGroupStatus procGroupStatus = context.getEventAccess().getControllerStatus();
        final String rootGroupName = procGroupStatus == null ? null : procGroupStatus.getName();
        final String nifiUrl = context.getProperty(SiteToSiteUtils.INSTANCE_URL).evaluateAttributeExpressions().getValue();
        URL url;
        try {
            url = URI.create(nifiUrl).toURL();
        } catch (IllegalArgumentException | MalformedURLException e) {
            // already validated
            throw new AssertionError();
        }

        final String hostname = url.getHost();
        final String platform = context.getProperty(SiteToSiteUtils.PLATFORM).evaluateAttributeExpressions().getValue();
        final Boolean allowNullValues = context.getProperty(ALLOW_NULL_VALUES).asBoolean();

        final Map<String, ?> config = Collections.emptyMap();
        final JsonBuilderFactory factory = Json.createBuilderFactory(config);
        final JsonObjectBuilder builder = factory.createObjectBuilder();

        final DateFormat df = new SimpleDateFormat(TIMESTAMP_FORMAT);
        df.setTimeZone(TimeZone.getTimeZone("Z"));

        consumer.consumeEvents(context, (mapHolder, events) -> {
            final long start = System.nanoTime();
            // Create a JSON array of all the events in the current batch
            final JsonArrayBuilder arrayBuilder = factory.createArrayBuilder();
            for (final ProvenanceEventRecord event : events) {
                final String componentName = mapHolder.getComponentName(event.getComponentId());
                final String processGroupId = mapHolder.getProcessGroupId(event.getComponentId(), event.getComponentType());
                final String processGroupName = mapHolder.getComponentName(processGroupId);
                arrayBuilder.add(serialize(factory, builder, event, df, componentName, processGroupId, processGroupName, hostname, url, rootGroupName, platform, nodeId, allowNullValues));
            }
            final JsonArray jsonArray = arrayBuilder.build();

            // Send the JSON document for the current batch
            Transaction transaction = null;
            try {
                // Lazily create SiteToSiteClient to provide a StateManager
                setup(context);

                transaction = getClient().createTransaction(TransferDirection.SEND);
                if (transaction == null) {
                    // Throw an exception to avoid provenance event id will not proceed so that those can be consumed again.
                    throw new ProcessException("All destination nodes are penalized; will attempt to send data later");
                }

                final Map<String, String> attributes = new HashMap<>();
                final String transactionId = UUID.randomUUID().toString();
                attributes.put("reporting.task.transaction.id", transactionId);
                attributes.put("reporting.task.name", getName());
                attributes.put("reporting.task.uuid", getIdentifier());
                attributes.put("reporting.task.type", this.getClass().getSimpleName());
                attributes.put("mime.type", "application/json");

                sendData(context, transaction, attributes, jsonArray);
                transaction.confirm();
                transaction.complete();

                final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                getLogger().info("Successfully sent {} Provenance Events to destination in {} ms; Transaction ID = {}; First Event ID = {}",
                        events.size(), transferMillis, transactionId, events.get(0).getEventId());
            } catch (final Exception e) {
                if (transaction != null) {
                    transaction.error();
                }
                if (e instanceof ProcessException) {
                    throw (ProcessException) e;
                } else {
                    throw new ProcessException("Failed to send Provenance Events to destination due to IOException:" + e.getMessage(), e);
                }
            }
        });

    }


    private JsonObject serialize(final JsonBuilderFactory factory, final JsonObjectBuilder builder, final ProvenanceEventRecord event, final DateFormat df,
            final String componentName, final String processGroupId, final String processGroupName, final String hostname, final URL nifiUrl, final String applicationName,
            final String platform, final String nodeIdentifier, Boolean allowNullValues) {
        addField(builder, "eventId", UUID.randomUUID().toString(), allowNullValues);
        addField(builder, "eventOrdinal", event.getEventId(), allowNullValues);
        addField(builder, "eventType", event.getEventType().name(), allowNullValues);
        addField(builder, "timestampMillis", event.getEventTime(), allowNullValues);
        addField(builder, "timestamp", df.format(event.getEventTime()), allowNullValues);
        addField(builder, "durationMillis", event.getEventDuration(), allowNullValues);
        addField(builder, "lineageStart", event.getLineageStartDate(), allowNullValues);
        addField(builder, "details", event.getDetails(), allowNullValues);
        addField(builder, "componentId", event.getComponentId(), allowNullValues);
        addField(builder, "componentType", event.getComponentType(), allowNullValues);
        addField(builder, "componentName", componentName, allowNullValues);
        addField(builder, "processGroupId", processGroupId, allowNullValues);
        addField(builder, "processGroupName", processGroupName, allowNullValues);
        addField(builder, "entityId", event.getFlowFileUuid(), allowNullValues);
        addField(builder, "entityType", "org.apache.nifi.flowfile.FlowFile", allowNullValues);
        addField(builder, "entitySize", event.getFileSize(), allowNullValues);
        addField(builder, "previousEntitySize", event.getPreviousFileSize(), allowNullValues);
        addField(builder, factory, "updatedAttributes", event.getUpdatedAttributes(), allowNullValues);
        addField(builder, factory, "previousAttributes", event.getPreviousAttributes(), allowNullValues);

        addField(builder, "actorHostname", hostname, allowNullValues);
        if (nifiUrl != null) {
            // TO get URL Prefix, we just remove the /nifi from the end of the URL. We know that the URL ends with
            // "/nifi" because the Property Validator enforces it
            final String urlString = nifiUrl.toString();
            final String urlPrefix = urlString.substring(0, urlString.length() - DESTINATION_URL_PATH.length());

            final String contentUriBase = urlPrefix + "/nifi-api/provenance-events/" + event.getEventId() + "/content/";
            final String nodeIdSuffix = nodeIdentifier == null ? "" : "?clusterNodeId=" + nodeIdentifier;
            addField(builder, "contentURI", contentUriBase + "output" + nodeIdSuffix, allowNullValues);
            addField(builder, "previousContentURI", contentUriBase + "input" + nodeIdSuffix, allowNullValues);
        }

        addField(builder, factory, "parentIds", event.getParentUuids(), allowNullValues);
        addField(builder, factory, "childIds", event.getChildUuids(), allowNullValues);
        addField(builder, "transitUri", event.getTransitUri(), allowNullValues);
        addField(builder, "remoteIdentifier", event.getSourceSystemFlowFileIdentifier(), allowNullValues);
        addField(builder, "alternateIdentifier", event.getAlternateIdentifierUri(), allowNullValues);
        addField(builder, "platform", platform, allowNullValues);
        addField(builder, "application", applicationName, allowNullValues);

        return builder.build();
    }

    private static void addField(final JsonObjectBuilder builder, final JsonBuilderFactory factory, final String key, final Map<String, String> values, Boolean allowNullValues) {
        if (values != null) {

            final JsonObjectBuilder mapBuilder = factory.createObjectBuilder();
            for (final Map.Entry<String, String> entry : values.entrySet()) {

                if (entry.getKey() == null ) {
                    continue;
                }else if(entry.getValue() == null ){
                    if(allowNullValues) {
                        mapBuilder.add(entry.getKey(), JsonValue.NULL);
                    }
                }else {
                    mapBuilder.add(entry.getKey(), entry.getValue());
                }
            }

            builder.add(key, mapBuilder);

        }else if(allowNullValues){
            builder.add(key,JsonValue.NULL);
        }
    }

    private void addField(final JsonObjectBuilder builder, final JsonBuilderFactory factory, final String key, final Collection<String> values, Boolean allowNullValues) {
        if (values != null) {
            builder.add(key, createJsonArray(factory, values));
        }else if(allowNullValues){
            builder.add(key,JsonValue.NULL);
        }
    }

    private static JsonArrayBuilder createJsonArray(JsonBuilderFactory factory, final Collection<String> values) {
        final JsonArrayBuilder builder = factory.createArrayBuilder();
        for (final String value : values) {
            if (value != null) {
                builder.add(value);
            }
        }
        return builder;
    }
}
