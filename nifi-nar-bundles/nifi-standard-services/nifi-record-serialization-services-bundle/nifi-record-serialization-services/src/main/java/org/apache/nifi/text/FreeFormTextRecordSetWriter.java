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

package org.apache.nifi.text;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Tags({"text", "freeform", "expression", "language", "el", "record", "recordset", "resultset", "writer", "serialize"})
@CapabilityDescription("Ghi nội dung của một RecordSet dưới dạng văn bản tự do. "
        + "Văn bản được cấu hình có thể sử dụng Expression Language để tham chiếu tới từng trường có trong Record, "
        + "cũng như các thuộc tính trong FlowFile và các biến. Nếu có trùng tên, tên/giá trị của trường sẽ được ưu tiên trước thuộc tính hoặc biến. "
        + "Mỗi bản ghi trong RecordSet sẽ được phân tách bằng một ký tự xuống dòng đơn.")
public class FreeFormTextRecordSetWriter extends AbstractControllerService implements RecordSetWriterFactory {
    static final PropertyDescriptor TEXT = new PropertyDescriptor.Builder()
        .name("Text")
        .description("Văn bản được sử dụng khi ghi kết quả. Thuộc tính này sẽ đánh giá Expression Language sử dụng bất kỳ trường nào có trong Record.")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(true)
        .build();
    static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
        .name("Character Set")
        .description("Bảng ký tự được sử dụng khi ghi dữ liệu vào FlowFile")
        .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
        .defaultValue("UTF-8")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .required(true)
        .build();

    private volatile PropertyValue textValue;
    private volatile Charset characterSet;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(TEXT);
        properties.add(CHARACTER_SET);
        return properties;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        textValue = context.getProperty(TEXT);
        characterSet = Charset.forName(context.getProperty(CHARACTER_SET).getValue());
    }

    @Override
    public RecordSchema getSchema(Map<String, String> variables, RecordSchema readSchema) throws SchemaNotFoundException, IOException {
        return readSchema;
    }

    @Override
    public RecordSetWriter createWriter(final ComponentLog logger, final RecordSchema schema, final OutputStream out, final Map<String, String> variables) {
        return new FreeFormTextWriter(textValue, characterSet, out, variables);
    }
}
