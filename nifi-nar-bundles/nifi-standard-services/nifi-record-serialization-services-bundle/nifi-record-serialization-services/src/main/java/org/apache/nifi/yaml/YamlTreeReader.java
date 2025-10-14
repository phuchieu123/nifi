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

package org.apache.nifi.yaml;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.json.JsonTreeRowRecordReader;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.inference.RecordSourceFactory;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

@Tags({"yaml", "tree", "record", "reader", "parser"})
@CapabilityDescription("Phân tích YAML thành các đối tượng Record riêng lẻ. Trong khi reader yêu cầu mỗi record phải là YAML hợp lệ, "
        + "nội dung của một FlowFile có thể gồm nhiều record, mỗi record là một mảng hoặc đối tượng YAML hợp lệ. "
        + "Nếu gặp một mảng, mỗi phần tử trong mảng sẽ được coi là một record riêng. "
        + "Nếu schema được cấu hình chứa một trường không có trong YAML, giá trị null sẽ được sử dụng. Nếu YAML chứa "
        + "một trường không có trong schema, trường đó sẽ bị bỏ qua. "
        + "Xin lưu ý, controller service này không hỗ trợ giải quyết các alias trong YAML. Bất kỳ alias nào xuất hiện sẽ được xử lý như một chuỗi. "
        + "Xem phần Hướng dẫn sử dụng Controller Service để biết thêm thông tin và ví dụ.")

public class YamlTreeReader extends JsonTreeReader {

    private static final boolean ALLOW_COMMENTS_DISABLED = false;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return new ArrayList<>(super.getSupportedPropertyDescriptors());
    }

    @Override
    protected RecordSourceFactory<JsonNode> createJsonRecordSourceFactory() {
        return (var, in) -> new YamlRecordSource(in, startingFieldStrategy, startingFieldName, streamReadConstraints);
    }

    @Override
    protected JsonTreeRowRecordReader createJsonTreeRowRecordReader(InputStream in, ComponentLog logger, RecordSchema schema) throws IOException, MalformedRecordException {
        return new YamlTreeRowRecordReader(in, logger, schema, dateFormat, timeFormat, timestampFormat, startingFieldStrategy, startingFieldName,
                schemaApplicationStrategy, null);
    }

    @Override
    protected boolean isAllowCommentsEnabled(final ConfigurationContext context) {
        return ALLOW_COMMENTS_DISABLED;
    }
}
