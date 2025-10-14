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

package org.apache.nifi.json;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.nifi.NullSuppression;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.DateTimeTextRecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.stream.io.GZIPOutputStream;
import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.XZOutputStream;
import org.xerial.snappy.SnappyFramedOutputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@Tags({"json", "resultset", "writer", "serialize", "record", "recordset", "row"})
@CapabilityDescription("Ghi kết quả của một RecordSet dưới dạng JSON Array hoặc một đối tượng JSON trên mỗi dòng. "
        + "Nếu sử dụng đầu ra Array, ngay cả khi RecordSet chỉ gồm một dòng, nó vẫn sẽ được ghi dưới dạng mảng với một phần tử. "
        + "Nếu sử dụng đầu ra One Line Per Object, các đối tượng JSON sẽ không thể được pretty-printed.")
public class JsonRecordSetWriter extends DateTimeTextRecordSetWriter implements RecordSetWriterFactory {

    public static final AllowableValue ALWAYS_SUPPRESS = new AllowableValue("always-suppress", "Luôn bỏ qua",
            "Các trường bị thiếu (có trong schema nhưng không có trong record), hoặc có giá trị null, sẽ không được ghi ra");
    public static final AllowableValue NEVER_SUPPRESS = new AllowableValue("never-suppress", "Không bao giờ bỏ qua",
            "Các trường bị thiếu (có trong schema nhưng không có trong record), hoặc có giá trị null, sẽ được ghi ra dưới dạng giá trị null");
    public static final AllowableValue SUPPRESS_MISSING = new AllowableValue("suppress-missing", "Bỏ qua giá trị thiếu",
            "Khi một trường có giá trị null, nó sẽ được ghi ra. Tuy nhiên, nếu trường được định nghĩa trong schema nhưng không có trong record, trường đó sẽ không được ghi ra.");

    public static final AllowableValue OUTPUT_ARRAY = new AllowableValue("output-array", "Mảng",
            "Xuất các record dưới dạng một mảng JSON");
    public static final AllowableValue OUTPUT_ONELINE = new AllowableValue("output-oneline", "Một dòng mỗi đối tượng",
            "Xuất các record với một đối tượng JSON trên mỗi dòng, ngăn cách bởi ký tự xuống dòng");

    public static final String COMPRESSION_FORMAT_GZIP = "gzip";
    public static final String COMPRESSION_FORMAT_BZIP2 = "bzip2";
    public static final String COMPRESSION_FORMAT_XZ_LZMA2 = "xz-lzma2";
    public static final String COMPRESSION_FORMAT_SNAPPY = "snappy";
    public static final String COMPRESSION_FORMAT_SNAPPY_FRAMED = "snappy framed";
    public static final String COMPRESSION_FORMAT_NONE = "none";
    public static final String COMPRESSION_FORMAT_ZSTD = "zstd";

    public static final PropertyDescriptor SUPPRESS_NULLS = new PropertyDescriptor.Builder()
            .name("suppress-nulls")
            .displayName("Bỏ qua giá trị null")
            .description("Xác định cách writer xử lý các trường null")
            .allowableValues(NEVER_SUPPRESS, ALWAYS_SUPPRESS, SUPPRESS_MISSING)
            .defaultValue(NEVER_SUPPRESS.getValue())
            .required(true)
            .build();

    public static final PropertyDescriptor PRETTY_PRINT_JSON = new PropertyDescriptor.Builder()
            .name("Pretty Print JSON")
            .description("Xác định JSON có được pretty-printed hay không")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    public static final PropertyDescriptor ALLOW_SCIENTIFIC_NOTATION = new PropertyDescriptor.Builder()
            .name("Allow Scientific Notation")
            .description("Xác định có sử dụng ký hiệu khoa học khi ghi số hay không")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    public static final PropertyDescriptor OUTPUT_GROUPING = new PropertyDescriptor.Builder()
            .name("output-grouping")
            .displayName("Nhóm đầu ra")
            .description("Xác định cách writer xuất các record JSON (dạng mảng hoặc một đối tượng mỗi dòng). "
                    + "Nếu chọn 'One Line Per Object', Pretty Print JSON phải là false.")
            .allowableValues(OUTPUT_ARRAY, OUTPUT_ONELINE)
            .defaultValue(OUTPUT_ARRAY.getValue())
            .required(true)
            .build();

    public static final PropertyDescriptor COMPRESSION_FORMAT = new PropertyDescriptor.Builder()
            .name("compression-format")
            .displayName("Định dạng nén")
            .description("Định dạng nén sử dụng. Các giá trị hợp lệ: GZIP, BZIP2, ZSTD, XZ-LZMA2, LZMA, Snappy, Snappy Framed")
            .allowableValues(COMPRESSION_FORMAT_NONE, COMPRESSION_FORMAT_GZIP, COMPRESSION_FORMAT_BZIP2, COMPRESSION_FORMAT_XZ_LZMA2,
                    COMPRESSION_FORMAT_SNAPPY, COMPRESSION_FORMAT_SNAPPY_FRAMED, COMPRESSION_FORMAT_ZSTD)
            .defaultValue(COMPRESSION_FORMAT_NONE)
            .required(true)
            .build();

    public static final PropertyDescriptor COMPRESSION_LEVEL = new PropertyDescriptor.Builder()
            .name("compression-level")
            .displayName("Mức độ nén")
            .description("Mức độ nén sử dụng; chỉ hợp lệ khi dùng GZIP. Giá trị thấp hơn sẽ xử lý nhanh hơn nhưng nén kém hơn; giá trị 0 không nén, chỉ lưu trữ.")
            .defaultValue("1")
            .required(true)
            .allowableValues("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
            .dependsOn(COMPRESSION_FORMAT, COMPRESSION_FORMAT_GZIP)
            .build();


    private volatile boolean prettyPrint;
    private volatile boolean allowScientificNotation;
    private volatile NullSuppression nullSuppression;
    private volatile OutputGrouping outputGrouping;
    private volatile String compressionFormat;
    private volatile int compressionLevel;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(PRETTY_PRINT_JSON);
        properties.add(SUPPRESS_NULLS);
        properties.add(ALLOW_SCIENTIFIC_NOTATION);
        properties.add(OUTPUT_GROUPING);
        properties.add(COMPRESSION_FORMAT);
        properties.add(COMPRESSION_LEVEL);
        return properties;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        final List<ValidationResult> problems = new ArrayList<>(super.customValidate(context));
        // Don't allow Pretty Print if One Line Per Object is selected
        if (context.getProperty(PRETTY_PRINT_JSON).asBoolean() && context.getProperty(OUTPUT_GROUPING).getValue().equals(OUTPUT_ONELINE.getValue())) {
            problems.add(new ValidationResult.Builder().input("Pretty Print").valid(false)
                    .explanation("Pretty Print JSON must be false when 'Output Grouping' is set to 'One Line Per Object'").build());
        }
        return problems;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        prettyPrint = context.getProperty(PRETTY_PRINT_JSON).asBoolean();
        allowScientificNotation = context.getProperty(ALLOW_SCIENTIFIC_NOTATION).asBoolean();

        final NullSuppression suppression;
        final String suppressNullValue = context.getProperty(SUPPRESS_NULLS).getValue();
        if (ALWAYS_SUPPRESS.getValue().equals(suppressNullValue)) {
            suppression = NullSuppression.ALWAYS_SUPPRESS;
        } else if (SUPPRESS_MISSING.getValue().equals(suppressNullValue)) {
            suppression = NullSuppression.SUPPRESS_MISSING;
        } else {
            suppression = NullSuppression.NEVER_SUPPRESS;
        }
        this.nullSuppression = suppression;

        String outputGroupingValue = context.getProperty(OUTPUT_GROUPING).getValue();
        final OutputGrouping grouping;
        if(OUTPUT_ONELINE.getValue().equals(outputGroupingValue)) {
            grouping = OutputGrouping.OUTPUT_ONELINE;
        } else {
            grouping = OutputGrouping.OUTPUT_ARRAY;
        }
        this.outputGrouping = grouping;

        this.compressionFormat = context.getProperty(COMPRESSION_FORMAT).getValue();
        this.compressionLevel = context.getProperty(COMPRESSION_LEVEL).asInteger();
    }

    @Override
    public RecordSetWriter createWriter(final ComponentLog logger, final RecordSchema schema, final OutputStream out, final Map<String, String> variables) throws SchemaNotFoundException, IOException {

        final OutputStream bufferedOut = new BufferedOutputStream(out, 65536);
        final OutputStream compressionOut;
        String mimeType;

        try {
            switch (compressionFormat.toLowerCase()) {
                case COMPRESSION_FORMAT_GZIP:
                    compressionOut = new GZIPOutputStream(bufferedOut, compressionLevel);
                    mimeType = "application/gzip";
                    break;
                case COMPRESSION_FORMAT_XZ_LZMA2:
                    compressionOut = new XZOutputStream(bufferedOut, new LZMA2Options());
                    mimeType = "application/x-xz";
                    break;
                case COMPRESSION_FORMAT_SNAPPY:
                    compressionOut = new SnappyOutputStream(bufferedOut);
                    mimeType = "application/x-snappy";
                    break;
                case COMPRESSION_FORMAT_SNAPPY_FRAMED:
                    compressionOut = new SnappyFramedOutputStream(bufferedOut);
                    mimeType = "application/x-snappy-framed";
                    break;
                case COMPRESSION_FORMAT_BZIP2:
                    mimeType = "application/x-bzip2";
                    compressionOut = new CompressorStreamFactory().createCompressorOutputStream(compressionFormat.toLowerCase(), bufferedOut);
                    break;
                case COMPRESSION_FORMAT_ZSTD:
                    mimeType = "application/zstd";
                    compressionOut = new CompressorStreamFactory().createCompressorOutputStream(compressionFormat.toLowerCase(), bufferedOut);
                    break;
                default:
                    mimeType = "application/json";
                    compressionOut = out;
            }
        } catch (CompressorException e) {
            throw new IOException(e);
        }

        return new WriteJsonResult(logger, schema, getSchemaAccessWriter(schema, variables), compressionOut, prettyPrint, nullSuppression, outputGrouping,
                getDateFormat().orElse(null), getTimeFormat().orElse(null), getTimestampFormat().orElse(null), mimeType, allowScientificNotation);
    }

}
