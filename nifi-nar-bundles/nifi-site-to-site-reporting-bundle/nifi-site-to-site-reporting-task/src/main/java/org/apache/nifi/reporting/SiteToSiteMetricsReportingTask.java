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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.json.Json;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;

import org.apache.avro.Schema;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.metrics.jvm.JmxJvmMetrics;
import org.apache.nifi.metrics.jvm.JvmMetrics;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.reporting.s2s.SiteToSiteUtils;
import org.apache.nifi.reporting.util.metrics.MetricNames;
import org.apache.nifi.reporting.util.metrics.MetricsService;
import org.apache.nifi.reporting.util.metrics.api.MetricsBuilder;

@Tags({"status", "metrics", "site", "site to site"})
@CapabilityDescription("Công bố các chỉ số tương tự như tác vụ Báo cáo Ambari bằng giao thức Site-to-Site.")
public class SiteToSiteMetricsReportingTask extends AbstractSiteToSiteReportingTask {

    static final AllowableValue AMBARI_FORMAT = new AllowableValue("ambari-format", "Định dạng Ambari", "Các chỉ số sẽ được định dạng"
            + " theo API Chỉ số Ambari. Xem Chi tiết Bổ sung trong tài liệu Sử dụng.");
    static final AllowableValue RECORD_FORMAT = new AllowableValue("record-format", "Định dạng Bản ghi", "Các chỉ số sẽ được định dạng"
            + " bằng cách sử dụng thuộc tính Record Writer của tác vụ báo cáo này. Xem Chi tiết Bổ sung trong tài liệu Sử dụng để"
            + " có mô tả về lược đồ mặc định.");

    static final PropertyDescriptor APPLICATION_ID = new PropertyDescriptor.Builder()
            .name("s2s-metrics-application-id")
            .displayName("ID Ứng dụng")
            .description("ID Ứng dụng sẽ được bao gồm trong các chỉ số")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("nifi")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
            .name("s2s-metrics-hostname")
            .displayName("Tên máy chủ")
            .description("Tên máy chủ của phiên bản NiFi này sẽ được bao gồm trong các chỉ số")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("${hostname(true)}")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor FORMAT = new PropertyDescriptor.Builder()
            .name("s2s-metrics-format")
            .displayName("Định dạng Đầu ra")
            .description("Định dạng đầu ra sẽ được sử dụng cho các chỉ số. Nếu " + RECORD_FORMAT.getDisplayName() + " được chọn, "
                    + "phải cung cấp một Record Writer. Nếu " + AMBARI_FORMAT.getDisplayName() + " được chọn, thuộc tính Record Writer "
                    + "nên để trống.")
            .required(true)
            .allowableValues(AMBARI_FORMAT, RECORD_FORMAT)
            .defaultValue(AMBARI_FORMAT.getValue())
            .addValidator(Validator.VALID)
            .build();

    private final MetricsService metricsService = new MetricsService();

    public SiteToSiteMetricsReportingTask() throws IOException {
        final InputStream schema = getClass().getClassLoader().getResourceAsStream("schema-metrics.avsc");
        recordSchema = AvroTypeUtil.createSchema(new Schema.Parser().parse(schema));
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(HOSTNAME);
        properties.add(APPLICATION_ID);
        properties.add(FORMAT);
        properties.remove(SiteToSiteUtils.BATCH_SIZE);
        return properties;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> problems = new ArrayList<>(super.customValidate(validationContext));

        final boolean isWriterSet = validationContext.getProperty(RECORD_WRITER).isSet();
        if (validationContext.getProperty(FORMAT).getValue().equals(RECORD_FORMAT.getValue()) && !isWriterSet) {
            problems.add(new ValidationResult.Builder()
                    .input("Record Writer")
                    .valid(false)
                    .explanation("If using " + RECORD_FORMAT.getDisplayName() + ", a record writer needs to be set.")
                    .build());
        }
        if (validationContext.getProperty(FORMAT).getValue().equals(AMBARI_FORMAT.getValue()) && isWriterSet) {
            problems.add(new ValidationResult.Builder()
                    .input("Record Writer")
                    .valid(false)
                    .explanation("If using " + AMBARI_FORMAT.getDisplayName() + ", no record writer should be set.")
                    .build());
        }

        return problems;
    }

    @Override
    public void onTrigger(final ReportingContext context) {
        final boolean isClustered = context.isClustered();
        final String nodeId = context.getClusterNodeIdentifier();
        if (nodeId == null && isClustered) {
            getLogger().debug("This instance of NiFi is configured for clustering, but the Cluster Node Identifier is not yet available. "
                    + "Will wait for Node Identifier to be established.");
            return;
        }

        final JvmMetrics virtualMachineMetrics = JmxJvmMetrics.getInstance();
        final Map<String, ?> config = Collections.emptyMap();
        final JsonBuilderFactory factory = Json.createBuilderFactory(config);

        final String applicationId = context.getProperty(APPLICATION_ID).evaluateAttributeExpressions().getValue();
        final String hostname = context.getProperty(HOSTNAME).evaluateAttributeExpressions().getValue();
        final ProcessGroupStatus status = context.getEventAccess().getControllerStatus();
        final Boolean allowNullValues = context.getProperty(ALLOW_NULL_VALUES).asBoolean();

        if(status != null) {
            final Map<String,String> statusMetrics = metricsService.getMetrics(status, false);
            final Map<String,String> jvmMetrics = metricsService.getMetrics(virtualMachineMetrics);

            final MetricsBuilder metricsBuilder = new MetricsBuilder(factory);
            final OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
            final double systemLoad = os.getSystemLoadAverage();

            byte[] data;
            final Map<String, String> attributes = new HashMap<>();

            if(context.getProperty(FORMAT).getValue().equals(AMBARI_FORMAT.getValue())) {
                final JsonObject metricsObject = metricsBuilder
                        .applicationId(applicationId)
                        .instanceId(status.getId())
                        .hostname(hostname)
                        .timestamp(System.currentTimeMillis())
                        .addAllMetrics(statusMetrics)
                        .addAllMetrics(jvmMetrics)
                        .metric(MetricNames.CORES, String.valueOf(os.getAvailableProcessors()))
                        .metric(MetricNames.LOAD1MN, String.valueOf(systemLoad >= 0 ? systemLoad : -1))
                        .build(allowNullValues);

                data = metricsObject.toString().getBytes(StandardCharsets.UTF_8);
                attributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");
            } else {
                final JsonObject metricsObject = metricsService.getMetrics(factory, status, virtualMachineMetrics, applicationId, status.getId(),
                        hostname, System.currentTimeMillis(), os.getAvailableProcessors(), systemLoad >= 0 ? systemLoad : -1, allowNullValues);
                data = getData(context, new ByteArrayInputStream(metricsObject.toString().getBytes(StandardCharsets.UTF_8)), attributes);
            }

            Transaction transaction = null;
            try {
                // Lazily create SiteToSiteClient to provide a StateManager
                setup(context);

                long start = System.nanoTime();
                transaction = getClient().createTransaction(TransferDirection.SEND);
                if (transaction == null) {
                    getLogger().debug("All destination nodes are penalized; will attempt to send data later");
                    return;
                }

                final String transactionId = UUID.randomUUID().toString();
                attributes.put("reporting.task.transaction.id", transactionId);
                attributes.put("reporting.task.name", getName());
                attributes.put("reporting.task.uuid", getIdentifier());
                attributes.put("reporting.task.type", this.getClass().getSimpleName());

                transaction.send(data, attributes);
                transaction.confirm();
                transaction.complete();

                final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                getLogger().info("Successfully sent metrics to destination in {}ms; Transaction ID = {}", transferMillis, transactionId);
            } catch (final Exception e) {
                if (transaction != null) {
                    transaction.error();
                }
                if (e instanceof ProcessException) {
                    throw (ProcessException) e;
                } else {
                    throw new ProcessException("Failed to send metrics to destination due to:" + e.getMessage(), e);
                }
            }

        } else {
            getLogger().error("No process group status to retrieve metrics");
        }
    }

}
