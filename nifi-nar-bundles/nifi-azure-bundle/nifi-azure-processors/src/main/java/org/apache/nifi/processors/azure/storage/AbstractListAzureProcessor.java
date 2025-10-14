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
package org.apache.nifi.processors.azure.storage;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.list.AbstractListProcessor;
import org.apache.nifi.processor.util.list.ListableEntity;

import java.util.concurrent.TimeUnit;

import static org.apache.nifi.processor.util.StandardValidators.TIME_PERIOD_VALIDATOR;

public abstract class AbstractListAzureProcessor<T extends ListableEntity> extends AbstractListProcessor<T> {
public static final PropertyDescriptor MIN_AGE = new PropertyDescriptor.Builder()
        .name("Minimum File Age")
        .description("Độ tuổi tối thiểu mà tệp phải đạt để được lấy; bất kỳ tệp nào có thời gian sửa đổi gần hơn mức này sẽ bị bỏ qua.")
        .required(true)
        .addValidator(TIME_PERIOD_VALIDATOR)
        .defaultValue("0 giây")
        .build();

public static final PropertyDescriptor MAX_AGE = new PropertyDescriptor.Builder()
        .name("Maximum File Age")
        .description("Độ tuổi tối đa mà tệp được phép có để được lấy; bất kỳ tệp nào cũ hơn mức này (dựa theo thời gian sửa đổi lần cuối) sẽ bị bỏ qua.")
        .required(false)
        .addValidator(StandardValidators.createTimePeriodValidator(100, TimeUnit.MILLISECONDS, Long.MAX_VALUE, TimeUnit.NANOSECONDS))
        .build();

public static final PropertyDescriptor MIN_SIZE = new PropertyDescriptor.Builder()
        .name("Minimum File Size")
        .description("Kích thước tối thiểu mà tệp phải đạt để được lấy.")
        .required(true)
        .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
        .defaultValue("0 B")
        .build();

public static final PropertyDescriptor MAX_SIZE = new PropertyDescriptor.Builder()
        .name("Maximum File Size")
        .description("Kích thước tối đa mà tệp được phép có để được lấy.")
        .required(false)
        .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
        .build();


    protected boolean isFileInfoMatchesWithAgeAndSize(final ProcessContext context, final long minimumTimestamp, final long lastModified, final long size) {
        final long minSize = context.getProperty(MIN_SIZE).asDataSize(DataUnit.B).longValue();
        final Double maxSize = context.getProperty(MAX_SIZE).asDataSize(DataUnit.B);
        final long minAge = context.getProperty(MIN_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        final Long maxAge = context.getProperty(MAX_AGE).asTimePeriod(TimeUnit.MILLISECONDS);

        if (lastModified < minimumTimestamp) {
            return false;
        }
        final long fileAge = System.currentTimeMillis() - lastModified;
        if (minAge > fileAge) {
            return false;
        }
        if (maxAge != null && maxAge < fileAge) {
            return false;
        }
        if (minSize > size) {
            return false;
        }
        if (maxSize != null && maxSize < size) {
            return false;
        }
        return true;
    }
}
