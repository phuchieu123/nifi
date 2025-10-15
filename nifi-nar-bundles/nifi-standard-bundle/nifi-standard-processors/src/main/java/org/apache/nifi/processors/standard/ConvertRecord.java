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
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.serialization.record.Record;

import java.util.ArrayList;
import java.util.List;

@EventDriven
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@SideEffectFree
@Tags({"convert", "record", "generic", "schema", "json", "csv", "avro", "log", "logs", "freeform", "text"})
@WritesAttributes({
    @WritesAttribute(attribute = "mime.type", description = "Đặt thuộc tính mime.type thành Loại MIME được chỉ định bởi Record Writer"),
    @WritesAttribute(attribute = "record.count", description = "Số lượng bản ghi trong FlowFile"),
    @WritesAttribute(attribute = "record.error.message", description = "Thuộc tính này cung cấp thông báo lỗi gặp phải bởi Reader hoặc Writer khi xảy ra lỗi.")
})
@CapabilityDescription("Chuyển đổi các bản ghi từ một định dạng dữ liệu này sang định dạng khác bằng cách sử dụng các Dịch vụ Điều khiển (Controller Services) Record Reader và Record Writer đã được cấu hình. "
    + "Reader và Writer phải được cấu hình với các lược đồ \"khớp nhau\". Điều này có nghĩa là các lược đồ phải có cùng tên trường. Kiểu của các trường "
    + "không nhất thiết phải giống nhau nếu giá trị của một trường có thể được ép kiểu từ kiểu này sang kiểu khác. Ví dụ, nếu lược đồ đầu vào có một trường tên là \"balance\" kiểu double, "
    + "lược đồ đầu ra có thể có một trường tên là \"balance\" với kiểu string, double, hoặc float. Nếu có bất kỳ trường nào trong đầu vào mà không có trong đầu ra, "
    + "trường đó sẽ bị bỏ qua ở đầu ra. Nếu có bất kỳ trường nào được chỉ định trong lược đồ đầu ra nhưng không có trong dữ liệu/lược đồ đầu vào, thì trường đó sẽ không có "
    + "trong đầu ra hoặc sẽ có giá trị null, tùy thuộc vào writer.")
public class ConvertRecord extends AbstractRecordProcessor {

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(INCLUDE_ZERO_RECORD_FLOWFILES);
        return properties;
    }

    @Override
    protected Record process(final Record record, final FlowFile flowFile, final ProcessContext context, final long count) {
        return record;
    }

}
