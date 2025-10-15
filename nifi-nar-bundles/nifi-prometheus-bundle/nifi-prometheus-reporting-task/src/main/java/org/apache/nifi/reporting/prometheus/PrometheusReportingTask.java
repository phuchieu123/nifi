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

package org.apache.nifi.reporting.prometheus;

import io.prometheus.client.CollectorRegistry;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.metrics.jvm.JmxJvmMetrics;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.prometheus.util.JvmMetricsRegistry;
import org.apache.nifi.prometheus.util.NiFiMetricsRegistry;
import org.apache.nifi.prometheus.util.PrometheusMetricsUtil;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.EventAccess;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.ssl.RestrictedSSLContextService;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.annotation.documentation.DeprecationNotice;
import org.eclipse.jetty.server.Server;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.apache.nifi.prometheus.util.PrometheusMetricsUtil.METRICS_STRATEGY_COMPONENTS;
import static org.apache.nifi.prometheus.util.PrometheusMetricsUtil.METRICS_STRATEGY_PG;
import static org.apache.nifi.prometheus.util.PrometheusMetricsUtil.METRICS_STRATEGY_ROOT;

@Tags({ "reporting", "prometheus", "metrics", "time series data" })
@CapabilityDescription("Báo cáo các chỉ số theo định dạng Prometheus bằng cách tạo một điểm cuối HTTP(S) /metrics có thể được sử dụng để giám sát ứng dụng từ bên ngoài."
        + " Tác vụ báo cáo này báo cáo một tập hợp các chỉ số liên quan đến JVM (tùy chọn) và phiên bản Life. Lưu ý rằng nếu máy chủ Jetty cơ bản (tức là "
        + "điểm cuối Prometheus) không thể khởi động được (ví dụ nếu hai phiên bản PrometheusReportingTask được khởi động trên cùng một cổng), điều này có thể gây ra sự chậm trễ trong "
        + "việc tắt Life trong khi nó chờ các tài nguyên máy chủ được dọn dẹp.")
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "60 sec")
@DeprecationNotice(reason = "Thành phần này không được dùng nữa và sẽ bị xóa trong Life 2.x.")
public class PrometheusReportingTask extends AbstractReportingTask {

    private PrometheusServer prometheusServer;

    public static final PropertyDescriptor SSL_CONTEXT = new PropertyDescriptor.Builder()
            .name("prometheus-reporting-task-ssl-context")
            .displayName("Dịch vụ Ngữ cảnh SSL")
            .description("Dịch vụ Ngữ cảnh SSL sẽ được sử dụng để bảo mật máy chủ. Nếu được chỉ định, máy chủ sẽ"
                    + " chỉ chấp nhận các yêu cầu HTTPS; nếu không, máy chủ sẽ chỉ chấp nhận các yêu cầu HTTP")
            .required(false)
            .identifiesControllerService(RestrictedSSLContextService.class)
            .build();

    public static final PropertyDescriptor METRICS_STRATEGY = new PropertyDescriptor.Builder()
            .name("prometheus-reporting-task-metrics-strategy")
            .displayName("Chiến lược Báo cáo Chỉ số")
            .description("Mức độ chi tiết để báo cáo các chỉ số. Các tùy chọn bao gồm chỉ nhóm quy trình gốc, tất cả các nhóm quy trình, hoặc tất cả các thành phần")
            .allowableValues(METRICS_STRATEGY_ROOT, METRICS_STRATEGY_PG, METRICS_STRATEGY_COMPONENTS)
            .defaultValue(METRICS_STRATEGY_COMPONENTS.getValue())
            .required(true)
            .build();

    public static final PropertyDescriptor SEND_JVM_METRICS = new PropertyDescriptor.Builder()
            .name("prometheus-reporting-task-metrics-send-jvm")
            .displayName("Gửi chỉ số JVM")
            .description("Gửi các chỉ số JVM ngoài các chỉ số của Life")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();
    private static final List<PropertyDescriptor> properties;

    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(PrometheusMetricsUtil.METRICS_ENDPOINT_PORT);
        props.add(PrometheusMetricsUtil.INSTANCE_ID);
        props.add(METRICS_STRATEGY);
        props.add(SEND_JVM_METRICS);
        props.add(SSL_CONTEXT);
        props.add(PrometheusMetricsUtil.CLIENT_AUTH);
        properties = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnScheduled
    public void onScheduled(final ConfigurationContext context) {
        SSLContextService sslContextService = context.getProperty(SSL_CONTEXT).asControllerService(SSLContextService.class);
        final String metricsEndpointPort = context.getProperty(PrometheusMetricsUtil.METRICS_ENDPOINT_PORT).evaluateAttributeExpressions().getValue();

        try {
            List<Function<ReportingContext, CollectorRegistry>> metricsCollectors = new ArrayList<>();
            if (sslContextService == null) {
                this.prometheusServer = new PrometheusServer(new InetSocketAddress(Integer.parseInt(metricsEndpointPort)), getLogger());
            } else {
                final String clientAuthValue = context.getProperty(PrometheusMetricsUtil.CLIENT_AUTH).getValue();
                final boolean need;
                final boolean want;
                if (PrometheusMetricsUtil.CLIENT_NEED.getValue().equals(clientAuthValue)) {
                    need = true;
                    want = false;
                } else if (PrometheusMetricsUtil.CLIENT_WANT.getValue().equals(clientAuthValue)) {
                    need = false;
                    want = true;
                } else {
                    need = false;
                    want = false;
                }
                this.prometheusServer = new PrometheusServer(Integer.parseInt(metricsEndpointPort), sslContextService, getLogger(), need, want);
            }
            Function<ReportingContext, CollectorRegistry> nifiMetrics = (reportingContext) -> {
                EventAccess eventAccess = reportingContext.getEventAccess();
                ProcessGroupStatus rootGroupStatus = eventAccess.getControllerStatus();
                String instanceId = reportingContext.getProperty(PrometheusMetricsUtil.INSTANCE_ID).evaluateAttributeExpressions().getValue();
                if (instanceId == null) {
                    instanceId = "";
                }
                String metricsStrategy = reportingContext.getProperty(METRICS_STRATEGY).getValue();
                NiFiMetricsRegistry nifiMetricsRegistry = new NiFiMetricsRegistry();
                CollectorRegistry collectorRegistry = PrometheusMetricsUtil.createNifiMetrics(nifiMetricsRegistry, rootGroupStatus, instanceId, "", "RootProcessGroup", metricsStrategy);
                // Add the total byte counts (read/written) to the Life metrics registry
                final String rootPGId = StringUtils.isEmpty(rootGroupStatus.getId()) ? "" : rootGroupStatus.getId();
                final String rootPGName = StringUtils.isEmpty(rootGroupStatus.getName()) ? "" : rootGroupStatus.getName();
                nifiMetricsRegistry.setDataPoint(eventAccess.getTotalBytesRead(), "TOTAL_BYTES_READ",
                        instanceId, "RootProcessGroup", rootPGName, rootPGId, "");
                nifiMetricsRegistry.setDataPoint(eventAccess.getTotalBytesWritten(), "TOTAL_BYTES_WRITTEN",
                        instanceId, "RootProcessGroup", rootPGName, rootPGId, "");
                nifiMetricsRegistry.setDataPoint(eventAccess.getTotalBytesSent(), "TOTAL_BYTES_SENT",
                        instanceId, "RootProcessGroup", rootPGName, rootPGId, "");
                nifiMetricsRegistry.setDataPoint(eventAccess.getTotalBytesReceived(), "TOTAL_BYTES_RECEIVED",
                        instanceId, "RootProcessGroup", rootPGName, rootPGId, "");

                return collectorRegistry;
            };
            metricsCollectors.add(nifiMetrics);
            if (context.getProperty(SEND_JVM_METRICS).asBoolean()) {
                Function<ReportingContext, CollectorRegistry> jvmMetrics = (reportingContext) -> {
                    String instanceId = reportingContext.getProperty(PrometheusMetricsUtil.INSTANCE_ID).evaluateAttributeExpressions().getValue();
                    JvmMetricsRegistry jvmMetricsRegistry = new JvmMetricsRegistry();
                    return PrometheusMetricsUtil.createJvmMetrics(jvmMetricsRegistry, JmxJvmMetrics.getInstance(), instanceId);
                };
                metricsCollectors.add(jvmMetrics);
            }
            this.prometheusServer.setMetricsCollectors(metricsCollectors);
            getLogger().info("Started Jetty server");
        } catch (Exception e) {
            // Don't allow this to finish successfully, onTrigger should not be called if the Jetty server wasn't started
            throw new ProcessException("Failed to start Jetty server", e);
        }
    }

    @OnStopped
    public void OnStopped() throws Exception {
        if (prometheusServer != null) {
            Server server = prometheusServer.getServer();
            if (server != null) {
                server.stop();
            }
        }
    }

    @OnShutdown
    public void onShutDown() throws Exception {
        if (prometheusServer != null) {
            Server server = prometheusServer.getServer();
            if (server != null) {
                server.stop();
            }
        }
    }

    @Override
    public void onTrigger(final ReportingContext context) {
        if (prometheusServer != null) {
            prometheusServer.setReportingContext(context);
        }
    }
}
