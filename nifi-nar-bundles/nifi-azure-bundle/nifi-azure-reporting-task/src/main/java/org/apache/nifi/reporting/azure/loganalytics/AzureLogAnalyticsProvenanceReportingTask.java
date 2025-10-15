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
package org.apache.nifi.reporting.azure.loganalytics;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.HttpPost;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.util.provenance.ProvenanceEventConsumer;

@Tags({ "azure", "provenace", "reporting", "log analytics" })
@CapabilityDescription("Công bố các sự kiện Provenance đến một không gian làm việc Azure Log Analytics.")
public class AzureLogAnalyticsProvenanceReportingTask extends AbstractAzureLogAnalyticsReportingTask {

        protected static final String LAST_EVENT_ID_KEY = "last_event_id";
        protected static final String DESTINATION_URL_PATH = "/du-lieu-lifetex";
        protected static final String TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

        static final PropertyDescriptor LOG_ANALYTICS_CUSTOM_LOG_NAME = new PropertyDescriptor.Builder()
                        .name("Log Analytics Custom Log Name").description("Tên Log Tùy chỉnh của Log Analytics").required(false)
                        .defaultValue("nifiprovenance").addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY).build();

        static final AllowableValue BEGINNING_OF_STREAM = new AllowableValue("beginning-of-stream",
                        "Bắt đầu Luồng",
                        "Bắt đầu đọc các Sự kiện Provenance từ đầu luồng (sự kiện cũ nhất trước tiên)");

        static final AllowableValue END_OF_STREAM = new AllowableValue("end-of-stream", "End of Stream",
                        "Bắt đầu đọc các Sự kiện Provenance từ cuối luồng, bỏ qua các sự kiện cũ");

        static final PropertyDescriptor FILTER_EVENT_TYPE = new PropertyDescriptor.Builder()
                        .name("s2s-prov-task-event-filter").displayName("Loại Sự kiện cần Bao gồm")
                        .description("Danh sách các loại sự kiện được phân tách bằng dấu phẩy sẽ được sử dụng để lọc các sự kiện provenance được gửi bởi tác vụ báo cáo. "
                                        + "Các loại sự kiện có sẵn là "
                                        + Arrays.deepToString(ProvenanceEventType.values())
                                        + ". Nếu không có bộ lọc nào được đặt, tất cả các sự kiện sẽ được gửi. Nếu "
                                        + "nhiều bộ lọc được đặt, các bộ lọc sẽ được cộng dồn.")
                        .required(false).expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

        static final PropertyDescriptor FILTER_EVENT_TYPE_EXCLUDE = new PropertyDescriptor.Builder()
                        .name("s2s-prov-task-event-filter-exclude").displayName("Loại Sự kiện cần Loại trừ")
                        .description("Danh sách các loại sự kiện được phân tách bằng dấu phẩy sẽ được sử dụng để loại trừ các sự kiện provenance được gửi bởi tác vụ báo cáo. "
                                        + "Các loại sự kiện có sẵn là "
                                        + Arrays.deepToString(ProvenanceEventType.values())
                                        + ". Nếu không có bộ lọc nào được đặt, tất cả các sự kiện sẽ được gửi. Nếu "
                                        + "nhiều bộ lọc được đặt, các bộ lọc sẽ được cộng dồn. Nếu một loại sự kiện được bao gồm trong Loại Sự kiện cần Bao gồm và bị loại trừ ở đây, thì "
                                        + "việc loại trừ sẽ được ưu tiên và sự kiện đó sẽ không được gửi.")
                        .required(false).expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

        static final PropertyDescriptor FILTER_COMPONENT_TYPE = new PropertyDescriptor.Builder()
                        .name("s2s-prov-task-type-filter").displayName("Loại Thành phần cần Bao gồm")
                        .description("Biểu thức chính quy để lọc các sự kiện provenance dựa trên loại thành phần. Chỉ những sự kiện khớp với biểu thức chính quy "
                                        + "mới được gửi. Nếu không có bộ lọc nào được đặt, tất cả các sự kiện sẽ được gửi. Nếu nhiều bộ lọc được đặt, các bộ lọc sẽ được cộng dồn.")
                        .required(false).expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                        .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR).build();

        static final PropertyDescriptor FILTER_COMPONENT_TYPE_EXCLUDE = new PropertyDescriptor.Builder()
                        .name("s2s-prov-task-type-filter-exclude").displayName("Loại Thành phần cần Loại trừ")
                        .description("Biểu thức chính quy để loại trừ các sự kiện provenance dựa trên loại thành phần. Các sự kiện khớp với biểu thức chính quy "
                                        + "sẽ không được gửi. Nếu không có bộ lọc nào được đặt, tất cả các sự kiện sẽ được gửi. Nếu nhiều bộ lọc được đặt, các bộ lọc sẽ được cộng dồn. "
                                        + "Nếu một loại thành phần được bao gồm trong Loại Thành phần cần Bao gồm và bị loại trừ ở đây, thì việc loại trừ sẽ được ưu tiên và sự kiện đó sẽ không được gửi.")
                        .required(false).expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                        .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR).build();

        static final PropertyDescriptor FILTER_COMPONENT_ID = new PropertyDescriptor.Builder()
                        .name("s2s-prov-task-id-filter").displayName("ID Thành phần cần Bao gồm")
                        .description("Danh sách các UUID của thành phần được phân tách bằng dấu phẩy sẽ được sử dụng để lọc các sự kiện provenance được gửi bởi tác vụ báo cáo. Nếu không có "
                                        + "bộ lọc nào được đặt, tất cả các sự kiện sẽ được gửi. Nếu nhiều bộ lọc được đặt, các bộ lọc sẽ được cộng dồn.")
                        .required(false).expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

        static final PropertyDescriptor FILTER_COMPONENT_ID_EXCLUDE = new PropertyDescriptor.Builder()
                        .name("s2s-prov-task-id-filter-exclude").displayName("ID Thành phần cần Loại trừ")
                        .description("Danh sách các UUID của thành phần được phân tách bằng dấu phẩy sẽ được sử dụng để loại trừ các sự kiện provenance được gửi bởi tác vụ báo cáo. Nếu không có "
                                        + "bộ lọc nào được đặt, tất cả các sự kiện sẽ được gửi. Nếu nhiều bộ lọc được đặt, các bộ lọc sẽ được cộng dồn. Nếu một UUID của thành phần được bao gồm trong "
                                        + "ID Thành phần cần Bao gồm và bị loại trừ ở đây, thì việc loại trừ sẽ được ưu tiên và sự kiện đó sẽ không được gửi.")
                        .required(false).expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

        static final PropertyDescriptor FILTER_COMPONENT_NAME = new PropertyDescriptor.Builder()
                        .name("s2s-prov-task-name-filter").displayName("Tên Thành phần cần Bao gồm")
                        .description("Biểu thức chính quy để lọc các sự kiện provenance dựa trên tên thành phần. Chỉ những sự kiện khớp với biểu thức chính quy "
                                        + "mới được gửi. Nếu không có bộ lọc nào được đặt, tất cả các sự kiện sẽ được gửi. Nếu nhiều bộ lọc được đặt, các bộ lọc sẽ được cộng dồn.")
                        .required(false).expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                        .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR).build();

        static final PropertyDescriptor FILTER_COMPONENT_NAME_EXCLUDE = new PropertyDescriptor.Builder()
                        .name("s2s-prov-task-name-filter-exclude").displayName("Tên Thành phần cần Loại trừ")
                        .description("Biểu thức chính quy để loại trừ các sự kiện provenance dựa trên tên thành phần. Các sự kiện khớp với biểu thức chính quy "
                                        + "sẽ không được gửi. Nếu không có bộ lọc nào được đặt, tất cả các sự kiện sẽ được gửi. Nếu nhiều bộ lọc được đặt, các bộ lọc sẽ được cộng dồn. "
                                        + "Nếu một tên thành phần được bao gồm trong Tên Thành phần cần Bao gồm và bị loại trừ ở đây, thì việc loại trừ sẽ được ưu tiên và sự kiện đó sẽ không được gửi.")
                        .required(false).expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                        .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR).build();

        static final PropertyDescriptor START_POSITION = new PropertyDescriptor.Builder().name("start-position")
                        .displayName("Vị trí Bắt đầu")
                        .description("Nếu Tác vụ Báo cáo chưa từng được chạy, hoặc nếu trạng thái của nó đã được người dùng đặt lại, "
                                        + "chỉ định nơi trong luồng Sự kiện Provenance mà Tác vụ Báo cáo nên bắt đầu")
                        .allowableValues(BEGINNING_OF_STREAM, END_OF_STREAM)
                        .defaultValue(BEGINNING_OF_STREAM.getValue()).required(true).build();

        static final PropertyDescriptor ALLOW_NULL_VALUES = new PropertyDescriptor.Builder().name("include-null-values")
                        .displayName("Bao gồm các Giá trị Null")
                        .description("Cho biết liệu các giá trị null có nên được bao gồm trong các bản ghi hay không. Mặc định sẽ là false")
                        .required(true).allowableValues("true", "false").defaultValue("false").build();

        static final PropertyDescriptor PLATFORM = new PropertyDescriptor.Builder().name("Platform")
                        .description("Giá trị sẽ được sử dụng cho trường platform trong mỗi sự kiện.").required(true)
                        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY).defaultValue("nifi")
                        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

        static final PropertyDescriptor INSTANCE_URL = new PropertyDescriptor.Builder().name("Instance URL")
                        .displayName("URL của Phiên bản")
                        .description("URL của phiên bản này sẽ được sử dụng trong Content URI của mỗi sự kiện.").required(true)
                        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                        .defaultValue("http://${hostname(true)}:8080/nifi")
                        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

        static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder().name("Batch Size")
                        .displayName("Kích thước Lô")
                        .description("Chỉ định số lượng bản ghi tối đa sẽ được gửi trong một lô duy nhất.").required(true)
                        .defaultValue("1000").addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).build();
        private volatile ProvenanceEventConsumer consumer;

        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
                final List<PropertyDescriptor> properties = new ArrayList<>();
                properties.add(LOG_ANALYTICS_WORKSPACE_ID);
                properties.add(LOG_ANALYTICS_CUSTOM_LOG_NAME);
                properties.add(LOG_ANALYTICS_WORKSPACE_KEY);
                properties.add(APPLICATION_ID);
                properties.add(INSTANCE_ID);
                properties.add(JOB_NAME);
                properties.add(LOG_ANALYTICS_URL_ENDPOINT_FORMAT);
                properties.add(FILTER_EVENT_TYPE);
                properties.add(FILTER_EVENT_TYPE_EXCLUDE);
                properties.add(FILTER_COMPONENT_TYPE);
                properties.add(FILTER_COMPONENT_TYPE_EXCLUDE);
                properties.add(FILTER_COMPONENT_ID);
                properties.add(FILTER_COMPONENT_ID_EXCLUDE);
                properties.add(FILTER_COMPONENT_NAME);
                properties.add(FILTER_COMPONENT_NAME_EXCLUDE);
                properties.add(START_POSITION);
                properties.add(ALLOW_NULL_VALUES);
                properties.add(PLATFORM);
                properties.add(INSTANCE_URL);
                properties.add(BATCH_SIZE);
                return properties;
        }

        public void CreateConsumer(final ReportingContext context) {
                if (consumer != null)
                        return;
                consumer = new ProvenanceEventConsumer();
                consumer.setStartPositionValue(context.getProperty(START_POSITION).getValue());
                consumer.setBatchSize(context.getProperty(BATCH_SIZE).asInteger());
                consumer.setLogger(getLogger());
                // initialize component type filtering
                consumer.setComponentTypeRegex(
                                context.getProperty(FILTER_COMPONENT_TYPE).evaluateAttributeExpressions().getValue());
                consumer.setComponentTypeRegexExclude(context.getProperty(FILTER_COMPONENT_TYPE_EXCLUDE)
                                .evaluateAttributeExpressions().getValue());
                consumer.setComponentNameRegex(
                                context.getProperty(FILTER_COMPONENT_NAME).evaluateAttributeExpressions().getValue());
                consumer.setComponentNameRegexExclude(context.getProperty(FILTER_COMPONENT_NAME_EXCLUDE)
                                .evaluateAttributeExpressions().getValue());

                final String[] targetEventTypes = StringUtils.stripAll(StringUtils.split(
                                context.getProperty(FILTER_EVENT_TYPE).evaluateAttributeExpressions().getValue(), ','));
                if (targetEventTypes != null) {
                        for (final String type : targetEventTypes) {
                                try {
                                        consumer.addTargetEventType(ProvenanceEventType.valueOf(type));
                                } catch (final Exception e) {
                                        getLogger().warn(type
                                                        + " is not a correct event type, removed from the filtering.");
                                }
                        }
                }

                final String[] targetEventTypesExclude = StringUtils
                                .stripAll(StringUtils.split(context.getProperty(FILTER_EVENT_TYPE_EXCLUDE)
                                                .evaluateAttributeExpressions().getValue(), ','));
                if (targetEventTypesExclude != null) {
                        for (final String type : targetEventTypesExclude) {
                                try {
                                        consumer.addTargetEventTypeExclude(ProvenanceEventType.valueOf(type));
                                } catch (final Exception e) {
                                        getLogger().warn(type
                                                        + " is not a correct event type, removed from the exclude filtering.");
                                }
                        }
                }

                // initialize component ID filtering
                final String[] targetComponentIds = StringUtils.stripAll(StringUtils.split(
                                context.getProperty(FILTER_COMPONENT_ID).evaluateAttributeExpressions().getValue(),
                                ','));
                if (targetComponentIds != null) {
                        consumer.addTargetComponentId(targetComponentIds);
                }

                final String[] targetComponentIdsExclude = StringUtils
                                .stripAll(StringUtils.split(context.getProperty(FILTER_COMPONENT_ID_EXCLUDE)
                                                .evaluateAttributeExpressions().getValue(), ','));
                if (targetComponentIdsExclude != null) {
                        consumer.addTargetComponentIdExclude(targetComponentIdsExclude);
                }

                consumer.setScheduled(true);
        }

        @Override
        public void onTrigger(ReportingContext context) {
                final boolean isClustered = context.isClustered();
                final String nodeId = context.getClusterNodeIdentifier();
                if (nodeId == null && isClustered) {
                        getLogger().debug(
                                        "This instance of NiFi is configured for clustering, but the Cluster Node Identifier is not yet available. "
                                                        + "Will wait for Node Identifier to be established.");
                        return;
                }

                try {
                        processProvenanceData(context);

                } catch (final Exception e) {
                        getLogger().error("Failed to publish metrics to Azure Log Analytics", e);
                }
        }

        public void processProvenanceData(final ReportingContext context) throws IOException {
                getLogger().debug("Starting to process provenance data");
                final String workspaceId = context.getProperty(LOG_ANALYTICS_WORKSPACE_ID)
                                .evaluateAttributeExpressions().getValue();
                final String linuxPrimaryKey = context.getProperty(LOG_ANALYTICS_WORKSPACE_KEY)
                                .evaluateAttributeExpressions().getValue();
                final String logName = context.getProperty(LOG_ANALYTICS_CUSTOM_LOG_NAME).evaluateAttributeExpressions()
                                .getValue();
                final String urlEndpointFormat = context.getProperty(LOG_ANALYTICS_URL_ENDPOINT_FORMAT)
                                .evaluateAttributeExpressions().getValue();
                final Integer batchSize = context.getProperty(BATCH_SIZE).asInteger();
                final String dataCollectorEndpoint = MessageFormat.format(urlEndpointFormat, workspaceId);
                final ProcessGroupStatus procGroupStatus = context.getEventAccess().getControllerStatus();
                final String nodeId = context.getClusterNodeIdentifier();
                final String rootGroupName = procGroupStatus == null ? null : procGroupStatus.getName();
                final String platform = context.getProperty(PLATFORM).evaluateAttributeExpressions().getValue();
                final Boolean allowNullValues = context.getProperty(ALLOW_NULL_VALUES).asBoolean();
                final String nifiUrl = context.getProperty(INSTANCE_URL).evaluateAttributeExpressions().getValue();
                URL url;
                try {
                        url = URI.create(nifiUrl).toURL();
                } catch (IllegalArgumentException | MalformedURLException e) {
                        throw new AssertionError();
                }

                final String hostname = url.getHost();
                final Map<String, Object> config = Collections.emptyMap();
                final JsonBuilderFactory factory = Json.createBuilderFactory(config);
                final JsonObjectBuilder builder = factory.createObjectBuilder();
                final DateFormat df = new SimpleDateFormat(TIMESTAMP_FORMAT);
                df.setTimeZone(TimeZone.getTimeZone("Z"));
                CreateConsumer(context);
                consumer.consumeEvents(context, (mapHolder, events) -> {
                        StringBuilder stringBuilder = new StringBuilder();
                        stringBuilder.append('[');
                        for (final ProvenanceEventRecord event : events) {
                                final String componentName = mapHolder.getComponentName(event.getComponentId());
                                final String processGroupId = mapHolder.getProcessGroupId(event.getComponentId(),
                                                event.getComponentType());
                                final String processGroupName = mapHolder.getComponentName(processGroupId);
                                final JsonObject jo = serialize(factory, builder, event, df, componentName,
                                                processGroupId, processGroupName, hostname, url, rootGroupName,
                                                platform, nodeId, allowNullValues);
                                stringBuilder.append(jo.toString());
                                stringBuilder.append(',');
                        }
                        if (stringBuilder.charAt(stringBuilder.length() - 1) == ',')
                                stringBuilder.deleteCharAt(stringBuilder.length() - 1);
                        stringBuilder.append(']');
                        String str = stringBuilder.toString();
                        if (!str.equals("[]")) {
                                final HttpPost httpPost = new HttpPost(dataCollectorEndpoint);
                                httpPost.addHeader("Content-Type", "application/json");
                                httpPost.addHeader("Log-Type", logName);
                                getLogger().debug("Sending " + batchSize + " events of length " + str.length() + " to azure log analytics " + logName);
                                try {
                                        sendToLogAnalytics(httpPost, workspaceId, linuxPrimaryKey, str);

                                } catch (final Exception e) {
                                        getLogger().error("Failed to publish provenance data to Azure Log Analytics", e);
                                }
                        }
                });
                getLogger().debug("Done processing provenance data");
        }

        private JsonObject serialize(final JsonBuilderFactory factory, final JsonObjectBuilder builder,
                        final ProvenanceEventRecord event, final DateFormat df, final String componentName,
                        final String processGroupId, final String processGroupName, final String hostname,
                        final URL nifiUrl, final String applicationName, final String platform,
                        final String nodeIdentifier, Boolean allowNullValues) {
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
                        // TO get URL Prefix, we just remove the /nifi from the end of the URL. We know
                        // that the URL ends with
                        // "/nifi" because the Property Validator enforces it
                        final String urlString = nifiUrl.toString();
                        final String urlPrefix = urlString.substring(0,
                                        urlString.length() - DESTINATION_URL_PATH.length());

                        final String contentUriBase = urlPrefix + "/nifi-api/provenance-events/" + event.getEventId()
                                        + "/content/";
                        final String nodeIdSuffix = nodeIdentifier == null ? "" : "?clusterNodeId=" + nodeIdentifier;
                        addField(builder, "contentURI", contentUriBase + "output" + nodeIdSuffix, allowNullValues);
                        addField(builder, "previousContentURI", contentUriBase + "input" + nodeIdSuffix,
                                        allowNullValues);
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

        @OnUnscheduled
        public void onUnscheduled() {
                if (consumer != null) {
                        getLogger().debug("Disabling schedule to consume provenance data.");
                        consumer.setScheduled(false);
                }
        }

        public static void addField(final JsonObjectBuilder builder, final String key, final Object value,
                        boolean allowNullValues) {
                if (value != null) {
                        if (value instanceof String) {
                                builder.add(key, (String) value);
                        } else if (value instanceof Integer) {
                                builder.add(key, (Integer) value);
                        } else if (value instanceof Boolean) {
                                builder.add(key, (Boolean) value);
                        } else if (value instanceof Long) {
                                builder.add(key, (Long) value);
                        } else {
                                builder.add(key, value.toString());
                        }
                } else if (allowNullValues) {
                        builder.add(key, JsonValue.NULL);
                }
        }

        public static void addField(final JsonObjectBuilder builder, final JsonBuilderFactory factory, final String key,
                        final Map<String, String> values, Boolean allowNullValues) {
                if (values != null) {
                        final JsonObjectBuilder mapBuilder = factory.createObjectBuilder();
                        for (final Map.Entry<String, String> entry : values.entrySet()) {

                                if (entry.getKey() == null) {
                                        continue;
                                } else if (entry.getValue() == null) {
                                        if (allowNullValues) {
                                                mapBuilder.add(entry.getKey(), JsonValue.NULL);
                                        }
                                } else {
                                        mapBuilder.add(entry.getKey(), entry.getValue());
                                }
                        }

                        builder.add(key, mapBuilder);

                } else if (allowNullValues) {
                        builder.add(key, JsonValue.NULL);
                }
        }

        public static void addField(final JsonObjectBuilder builder, final JsonBuilderFactory factory, final String key,
                        final Collection<String> values, Boolean allowNullValues) {
                if (values != null) {
                        builder.add(key, createJsonArray(factory, values));
                } else if (allowNullValues) {
                        builder.add(key, JsonValue.NULL);
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
