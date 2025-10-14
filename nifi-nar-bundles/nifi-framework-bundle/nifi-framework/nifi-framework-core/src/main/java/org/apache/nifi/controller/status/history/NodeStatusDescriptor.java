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
package org.apache.nifi.controller.status.history;

import org.apache.nifi.controller.status.NodeStatus;

import java.util.List;
import java.util.Objects;

public enum NodeStatusDescriptor {
    FREE_HEAP(
            "freeHeap",
            "Bộ nhớ Heap trống",
            "Lượng bộ nhớ trống trong heap mà máy ảo Java có thể sử dụng.",
            MetricDescriptor.Formatter.DATA_SIZE,
            s -> s.getFreeHeap()),
    USED_HEAP(
            "usedHeap",
            "Bộ nhớ Heap đã dùng",
            "Lượng bộ nhớ được sử dụng trong heap được máy ảo Java sử dụng.",
            MetricDescriptor.Formatter.DATA_SIZE,
            s -> s.getUsedHeap()),
    HEAP_UTILIZATION(
            "heapUtilization",
            "Mức sử dụng Heap",
            "Tỷ lệ phần trăm heap khả dụng hiện đang được máy ảo Java sử dụng.",
            MetricDescriptor.Formatter.COUNT,
            s -> s.getHeapUtilization(),
            new ValueReducer<StatusSnapshot, Long>() {
                @Override
                public Long reduce(final List<StatusSnapshot> values) {
                    return (long) values.stream()
                            .map(snapshot -> snapshot.getStatusMetric(HEAP_UTILIZATION.getDescriptor()))
                            .filter(Objects::nonNull)
                            .mapToLong(value -> value)
                            .average()
                            .orElse(0L);
                }
            }),
    FREE_NON_HEAP(
            "freeNonHeap",
            "Bộ nhớ Non-Heap trống",
            "Bộ nhớ không phải heap hiện có sẵn có thể được máy ảo Java sử dụng.",
            MetricDescriptor.Formatter.DATA_SIZE,
            s -> s.getFreeNonHeap()),
    USED_NON_HEAP(
            "usedNonHeap",
            "Bộ nhớ Non-Heap đã dùng",
            "Mức sử dụng bộ nhớ không phải heap hiện tại được máy ảo Java sử dụng.",
            MetricDescriptor.Formatter.DATA_SIZE,
            s -> s.getUsedNonHeap()),
    OPEN_FILE_HANDLES(
            "openFileHandles",
            "Số lượng File Handle mở",
            "Số lượng hiện tại của các xử lý tệp mở được sử dụng bởi máy ảo Java.",
            MetricDescriptor.Formatter.COUNT,
            s -> s.getOpenFileHandlers()),
    PROCESSOR_LOAD_AVERAGE(
            "processorLoadAverage",
            "Tải trung bình của bộ xử lý",
            "Tải bộ xử lý. Mỗi điểm đo thể hiện mức tải trung bình của hệ thống trong phút cuối cùng.",
            MetricDescriptor.Formatter.FRACTION,
            s -> Double.valueOf(s.getProcessorLoadAverage() * MetricDescriptor.FRACTION_MULTIPLIER).longValue(),
            new ValueReducer<StatusSnapshot, Long>() {
                @Override
                public Long reduce(final List<StatusSnapshot> values) {
                    return (long) values.stream()
                            .map(snapshot -> snapshot.getStatusMetric(HEAP_UTILIZATION.getDescriptor()))
                            .filter(Objects::nonNull)
                            .mapToLong(value -> value)
                            .average()
                            .orElse(0L);
                }
            }),
    TOTAL_THREADS(
            "totalThreads",
            "Tổng số luồng",
            "Số lượng luồng trực tiếp hiện tại trong máy ảo Java (cả luồng daemon và luồng không phải daemon).",
            MetricDescriptor.Formatter.COUNT,
            s -> s.getTotalThreads()),
    EVENT_DRIVEN_THREADS(
            "eventDrivenThreads",
            "Số luồng điều khiển sự kiện",
            "Số lượng luồng đang hoạt động hiện tại trong nhóm luồng điều khiển sự kiện.",
            MetricDescriptor.Formatter.COUNT,
            s -> s.getEventDrivenThreads()),
    TIME_DRIVEN_THREADS(
            "timeDrivenThreads",
            "Số luồng điều khiển theo thời gian",
            "Số lượng luồng đang hoạt động hiện tại trong nhóm luồng theo thời gian.",
            MetricDescriptor.Formatter.COUNT,
            s -> s.getTimerDrivenThreads()),
    FLOW_FILE_REPOSITORY_FREE_SPACE(
            "flowFileRepositoryFreeSpace",
            "Dung lượng trống kho FlowFile",
            "Không gian có thể sử dụng cho kho lưu trữ tệp trên cơ chế lưu trữ cơ bản",
            MetricDescriptor.Formatter.DATA_SIZE,
            s -> s.getFlowFileRepositoryFreeSpace()),
    FLOW_FILE_REPOSITORY_USED_SPACE(
            "flowFileRepositoryUsedSpace",
            "Dung lượng đã dùng kho FlowFile",
            "Không gian được sử dụng trên cơ chế lưu trữ cơ bản.",
            MetricDescriptor.Formatter.DATA_SIZE,
            s -> s.getFlowFileRepositoryUsedSpace()),
    CONTENT_REPOSITORY_FREE_SPACE(
            "contentRepositoryFreeSpace",
            "Tổng dung lượng trống kho Nội dung",
            "Không gian có thể sử dụng cho kho lưu trữ nội dung trên cơ chế lưu trữ cơ bản.",
            MetricDescriptor.Formatter.DATA_SIZE,
            s -> s.getContentRepositories().stream().mapToLong(r -> r.getFreeSpace()).sum()),
    CONTENT_REPOSITORY_USED_SPACE(
            "contentRepositoryUsedSpace",
            "Tổng dung lượng đã dùng kho Nội dung",
            "Không gian được sử dụng trên các cơ chế lưu trữ cơ bản.",
            MetricDescriptor.Formatter.DATA_SIZE,
            s -> s.getContentRepositories().stream().mapToLong(r -> r.getUsedSpace()).sum()),
    PROVENANCE_REPOSITORY_FREE_SPACE(
            "provenanceRepositoryFreeSpace",
            "Tổng dung lượng trống kho Provenance",
            "Không gian có thể sử dụng cho kho lưu trữ Provenance trên cơ chế lưu trữ cơ bản.",
            MetricDescriptor.Formatter.DATA_SIZE,
            s -> s.getProvenanceRepositories().stream().mapToLong(r -> r.getFreeSpace()).sum()),
    PROVENANCE_REPOSITORY_USED_SPACE(
            "provenanceRepositoryUsedSpace",
            "Tổng dung lượng đã dùng kho Provenance",
            "Không gian được sử dụng trên các cơ chế lưu trữ cơ bản.",
            MetricDescriptor.Formatter.DATA_SIZE,
            s -> s.getProvenanceRepositories().stream().mapToLong(r -> r.getUsedSpace()).sum());

    private final MetricDescriptor<NodeStatus> descriptor;

    NodeStatusDescriptor(
            final String field,
            final String label,
            final String description,
            final MetricDescriptor.Formatter formatter,
            final ValueMapper<NodeStatus> valueFunction) {
        this.descriptor = new StandardMetricDescriptor<>(this::ordinal, field, label, description, formatter, valueFunction);
    }

    NodeStatusDescriptor(
            final String field,
            final String label,
            final String description,
            final MetricDescriptor.Formatter formatter,
            final ValueMapper<NodeStatus> valueFunction,
            final ValueReducer<StatusSnapshot, Long> reducer) {
        this.descriptor = new StandardMetricDescriptor<>(this::ordinal, field, label, description, formatter, valueFunction, reducer);
    }

    public String getField() {
        return descriptor.getField();
    }

    public MetricDescriptor<NodeStatus> getDescriptor() {
        return descriptor;
    }
}
