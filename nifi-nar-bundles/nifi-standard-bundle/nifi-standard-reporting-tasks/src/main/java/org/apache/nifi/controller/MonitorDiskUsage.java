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
package org.apache.nifi.controller;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.util.FormatUtils;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Tags({"disk", "storage", "warning", "monitoring", "repo"})
@CapabilityDescription("Kiểm tra dung lượng lưu trữ còn trống cho thư mục được chỉ định"
        + " và cảnh báo (thông qua một thông điệp log và một Thông báo Cấp Hệ thống) nếu phân vùng chứa nó vượt quá"
        + " một ngưỡng dung lượng lưu trữ có thể cấu hình")
public class MonitorDiskUsage extends AbstractReportingTask {

    private static final Pattern PERCENT_PATTERN = Pattern.compile("(\\d+{1,2})%");

    public static final PropertyDescriptor DIR_THRESHOLD = new PropertyDescriptor.Builder()
            .name("Threshold")
            .description("Ngưỡng mà tại đó một thông báo sẽ được tạo ra để chỉ ra rằng việc sử dụng đĩa của phân vùng chứa thư mục được tìm thấy là đáng lo ngại")
            .required(true)
            .addValidator(StandardValidators.createRegexMatchingValidator(PERCENT_PATTERN))
            .defaultValue("80%")
            .build();

    public static final PropertyDescriptor DIR_LOCATION = new PropertyDescriptor.Builder()
            .name("Directory Location")
            .description("Đường dẫn thư mục của phân vùng cần được giám sát.")
            .required(true)
            .addValidator(StandardValidators.createDirectoryExistsValidator(false, false))
            .build();

    public static final PropertyDescriptor DIR_DISPLAY_NAME = new PropertyDescriptor.Builder()
            .name("Directory Display Name")
            .description("Tên sẽ được hiển thị cho thư mục trong các cảnh báo.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue("Chưa được đặt tên")
            .build();
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>(2);
        descriptors.add(DIR_THRESHOLD);
        descriptors.add(DIR_LOCATION);
        descriptors.add(DIR_DISPLAY_NAME);
        return descriptors;
    }

    @Override
    public void onTrigger(final ReportingContext context) {
        final String thresholdValue = context.getProperty(DIR_THRESHOLD).getValue();
        final Matcher thresholdMatcher = PERCENT_PATTERN.matcher(thresholdValue.trim());
        thresholdMatcher.find();
        final String thresholdPercentageVal = thresholdMatcher.group(1);
        final int contentRepoThreshold = Integer.parseInt(thresholdPercentageVal);

        final File dir = new File(context.getProperty(DIR_LOCATION).getValue());
        final String dirName = context.getProperty(DIR_DISPLAY_NAME).getValue();

        checkThreshold(dirName, dir.toPath(), contentRepoThreshold, getLogger());

    }

    static void checkThreshold(final String pathName, final Path path, final int threshold, final ComponentLog logger) {
        final File file = path.toFile();
        final long totalBytes = file.getTotalSpace();
        final long freeBytes = file.getFreeSpace();
        final long usedBytes = totalBytes - freeBytes;

        final double usedPercent = (double) usedBytes / (double) totalBytes * 100D;

        if (usedPercent >= threshold) {
            final String usedSpace = FormatUtils.formatDataSize(usedBytes);
            final String totalSpace = FormatUtils.formatDataSize(totalBytes);
            final String freeSpace = FormatUtils.formatDataSize(freeBytes);

            final double freePercent = (double) freeBytes / (double) totalBytes * 100D;

            final String message = String.format("%1$s exceeds configured threshold of %2$s%%, having %3$s / %4$s (%5$.2f%%) used and %6$s (%7$.2f%%) free",
                    pathName, threshold, usedSpace, totalSpace, usedPercent, freeSpace, freePercent);
            logger.warn(message);
        }
    }
}
