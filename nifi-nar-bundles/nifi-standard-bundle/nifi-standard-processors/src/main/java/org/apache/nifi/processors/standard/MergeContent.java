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

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.flowfile.attributes.StandardFlowFileMediaType;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.bin.Bin;
import org.apache.nifi.processor.util.bin.BinFiles;
import org.apache.nifi.processor.util.bin.BinManager;
import org.apache.nifi.processor.util.bin.BinProcessingResult;
import org.apache.nifi.processors.standard.merge.AttributeStrategy;
import org.apache.nifi.processors.standard.merge.AttributeStrategyUtil;
import org.apache.nifi.stream.io.NonCloseableOutputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.FlowFilePackager;
import org.apache.nifi.util.FlowFilePackagerV1;
import org.apache.nifi.util.FlowFilePackagerV2;
import org.apache.nifi.util.FlowFilePackagerV3;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipOutputStream;

@SideEffectFree
@TriggerWhenEmpty
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"merge", "content", "correlation", "tar", "zip", "stream", "concatenation", "archive", "flowfile-stream", "flowfile-stream-v3"})
@CapabilityDescription("Gộp một Nhóm FlowFile lại với nhau dựa trên một chiến lược do người dùng xác định và đóng gói chúng thành một FlowFile duy nhất. "
        + "Khuyến nghị rằng Bộ xử lý chỉ được cấu hình với một kết nối đến duy nhất, vì Nhóm FlowFile sẽ không được tạo từ các FlowFile trong các kết nối khác nhau. "
        + "Bộ xử lý này cập nhật thuộc tính mime.type một cách thích hợp. "
        + "LƯU Ý: bộ xử lý này KHÔNG nên được cấu hình với Cron Driven cho Chiến lược Lập lịch.")
@ReadsAttributes({
    @ReadsAttribute(attribute = "fragment.identifier", description = "Chỉ áp dụng nếu thuộc tính <Chiến lược hợp nhất> được đặt thành Chống phân mảnh. "
        + "Tất cả các FlowFile có cùng giá trị cho thuộc tính này sẽ được gộp lại với nhau."),
    @ReadsAttribute(attribute = "fragment.index", description = "Chỉ áp dụng nếu thuộc tính <Chiến lược hợp nhất> được đặt thành Chống phân mảnh. "
        + "Thuộc tính này cho biết thứ tự các mảnh sẽ được lắp ráp. Thuộc tính này "
        + "phải có mặt trên tất cả các FlowFile khi sử dụng Chiến lược hợp nhất Chống phân mảnh và phải là một số nguyên duy nhất (tức là duy nhất trên tất cả "
        + "các FlowFile có cùng giá trị cho thuộc tính \"fragment.identifier\") "
        + "trong khoảng từ 0 đến giá trị của thuộc tính fragment.count. Nếu hai hoặc nhiều FlowFile có cùng giá trị cho "
        + "thuộc tính \"fragment.identifier\" và cùng giá trị cho thuộc tính \"fragment.index\", FlowFile đầu tiên được xử lý sẽ được "
        + "chấp nhận và các FlowFile tiếp theo sẽ không được chấp nhận vào Bin."),
    @ReadsAttribute(attribute = "fragment.count", description = "Chỉ áp dụng nếu thuộc tính <Chiến lược hợp nhất> được đặt thành Chống phân mảnh. Thuộc tính này "
        + "cho biết có bao nhiêu FlowFile được mong đợi trong gói đã cho. Ít nhất một FlowFile phải có thuộc tính này trong "
        + "gói. Nếu nhiều FlowFile chứa thuộc tính \"fragment.count\" trong một gói nhất định, tất cả phải có cùng một giá trị."),
    @ReadsAttribute(attribute = "segment.original.filename", description = "Chỉ áp dụng nếu thuộc tính <Chiến lược hợp nhất> được đặt thành Chống phân mảnh. "
        + "Thuộc tính này phải có mặt trên tất cả các FlowFile có cùng giá trị cho thuộc tính fragment.identifier. Tất cả các FlowFile trong cùng một "
        + "gói phải có cùng giá trị cho thuộc tính này. Giá trị của thuộc tính này sẽ được sử dụng cho tên tệp của FlowFile đã được hợp nhất hoàn chỉnh."),
    @ReadsAttribute(attribute = "tar.permissions", description = "Chỉ áp dụng nếu thuộc tính <Định dạng hợp nhất> được đặt thành TAR. Giá trị của thuộc tính này "
        + "phải là 3 ký tự; mỗi ký tự phải nằm trong khoảng từ 0 đến 7 (bao gồm) và cho biết quyền truy cập tệp sẽ "
        + "được sử dụng cho mục TAR của FlowFile. Nếu thuộc tính này bị thiếu hoặc có giá trị không hợp lệ, giá trị mặc định là 644 sẽ được sử dụng") })
@WritesAttributes({
    @WritesAttribute(attribute = "filename", description = "Khi có nhiều hơn 1 tệp được hợp nhất, tên tệp đến từ thuộc tính segment.original.filename. "
        + "Nếu thuộc tính đó không tồn tại trong các FlowFile nguồn, thì tên tệp được đặt thành số nano giây khớp với thời gian hệ thống. Sau đó, một phần mở rộng tên tệp có thể được áp dụng:"
        + "nếu Định dạng hợp nhất là TAR, thì tên tệp sẽ được nối thêm .tar, "
        + "nếu Định dạng hợp nhất là ZIP, thì tên tệp sẽ được nối thêm .zip, "
        + "nếu Định dạng hợp nhất là FlowFileStream, thì tên tệp sẽ được nối thêm .pkg"),
    @WritesAttribute(attribute = "merge.count", description = "Số lượng FlowFile đã được hợp nhất vào gói này"),
    @WritesAttribute(attribute = "merge.bin.age", description = "Tuổi của bin, tính bằng mili giây, khi nó được hợp nhất và xuất ra. Thực chất "
        + "đây là khoảng thời gian lớn nhất mà bất kỳ FlowFile nào trong gói này vẫn chờ đợi trong bộ xử lý này trước khi được xuất ra"),
    @WritesAttribute(attribute = "merge.uuid", description = "UUID của flow file đã hợp nhất sẽ được thêm vào các thuộc tính của flow file gốc."),
    @WritesAttribute(attribute = "merge.reason", description = "Bộ xử lý này cho phép cấu hình một số ngưỡng để hợp nhất các FlowFile. Thuộc tính này cho biết ngưỡng nào" +
        " đã dẫn đến việc các FlowFile được hợp nhất. Để giải thích về từng giá trị có thể có và ý nghĩa của chúng, hãy xem phần Sử dụng / tài liệu của Bộ xử lý và xem trang 'Chi tiết bổ sung'.")
})
@SeeAlso({SegmentContent.class, MergeRecord.class})
@SystemResourceConsideration(resource = SystemResource.MEMORY, description = "Mặc dù nội dung không được lưu trữ trong bộ nhớ, các thuộc tính của FlowFile thì có. " +
        "Cấu hình của MergeContent (kích thước bin tối đa, kích thước nhóm tối đa, tuổi bin tối đa, số lượng mục nhập tối đa) sẽ ảnh hưởng đến lượng bộ nhớ được sử dụng. " +
        "Nếu hợp nhất nhiều FlowFile nhỏ lại với nhau, có thể cần một phương pháp tiếp cận hai giai đoạn để tránh sử dụng bộ nhớ quá mức.")
public class MergeContent extends BinFiles {

    // thuộc tính ưu tiên
    public static final String FRAGMENT_ID_ATTRIBUTE = FragmentAttributes.FRAGMENT_ID.key();
    public static final String FRAGMENT_INDEX_ATTRIBUTE = FragmentAttributes.FRAGMENT_INDEX.key();
    public static final String FRAGMENT_COUNT_ATTRIBUTE = FragmentAttributes.FRAGMENT_COUNT.key();

    // thuộc tính kiểu cũ
    public static final String SEGMENT_ID_ATTRIBUTE = "segment.identifier";
    public static final String SEGMENT_INDEX_ATTRIBUTE = "segment.index";
    public static final String SEGMENT_COUNT_ATTRIBUTE = "segment.count";
    public static final String SEGMENT_ORIGINAL_FILENAME = FragmentAttributes.SEGMENT_ORIGINAL_FILENAME.key();


    public static final AllowableValue METADATA_STRATEGY_USE_FIRST = new AllowableValue("Sử dụng siêu dữ liệu đầu tiên", "Sử dụng siêu dữ liệu đầu tiên",
            "Đối với bất kỳ định dạng đầu vào nào hỗ trợ siêu dữ liệu (ví dụ: Avro), siêu dữ liệu cho FlowFile đầu tiên trong bin sẽ được đặt trên FlowFile đầu ra.");

    public static final AllowableValue METADATA_STRATEGY_ALL_COMMON = new AllowableValue("Chỉ giữ lại siêu dữ liệu chung", "Chỉ giữ lại siêu dữ liệu chung",
            "Đối với bất kỳ định dạng đầu vào nào hỗ trợ siêu dữ liệu (ví dụ: Avro), bất kỳ FlowFile nào có giá trị siêu dữ liệu khớp với giá trị của FlowFile đầu tiên, mọi siêu dữ liệu bổ sung "
                    + "sẽ bị loại bỏ nhưng FlowFile sẽ được hợp nhất. Bất kỳ FlowFile nào có giá trị siêu dữ liệu không khớp với giá trị của FlowFile đầu tiên trong bin sẽ không được hợp nhất.");

    public static final AllowableValue METADATA_STRATEGY_IGNORE = new AllowableValue("Bỏ qua siêu dữ liệu", "Bỏ qua siêu dữ liệu",
            "Bỏ qua (không chuyển, so sánh, v.v.) bất kỳ siêu dữ liệu nào từ một FlowFile có nội dung hỗ trợ siêu dữ liệu nhúng.");

    public static final AllowableValue METADATA_STRATEGY_DO_NOT_MERGE = new AllowableValue("Không hợp nhất siêu dữ liệu không phổ biến", "Không hợp nhất siêu dữ liệu không phổ biến",
            "Đối với bất kỳ định dạng đầu vào nào hỗ trợ siêu dữ liệu (ví dụ: Avro), bất kỳ FlowFile nào có giá trị siêu dữ liệu không khớp với giá trị của FlowFile đầu tiên trong bin sẽ không được hợp nhất.");

    public static final AllowableValue MERGE_STRATEGY_BIN_PACK = new AllowableValue(
            "Thuật toán Đóng gói Bin",
            "Thuật toán Đóng gói Bin",
            "Tạo ra các 'bin' của FlowFile và lấp đầy mỗi bin càng đầy càng tốt. Các FlowFile được đặt vào một bin dựa trên kích thước của chúng và tùy chọn "
            + "các thuộc tính của chúng (nếu thuộc tính <Thuộc tính tương quan> được đặt)");
    public static final AllowableValue MERGE_STRATEGY_DEFRAGMENT = new AllowableValue(
            "Chống phân mảnh",
            "Chống phân mảnh",
            "Kết hợp các mảnh được liên kết bởi các thuộc tính trở lại thành một FlowFile gắn kết duy nhất. Nếu sử dụng chiến lược này, tất cả các FlowFile phải "
            + "có các thuộc tính <fragment.identifier>, <fragment.count>, và <fragment.index> hoặc thay thế (cho mục đích tương thích ngược) "
            + "<segment.identifier>, <segment.count>, và <segment.index>. Tất cả các FlowFile có cùng giá trị cho \"fragment.identifier\" "
            + "sẽ được nhóm lại với nhau. Tất cả các FlowFile trong nhóm này phải có cùng giá trị cho thuộc tính \"fragment.count\". Tất cả các FlowFile "
            + "trong nhóm này phải có một giá trị duy nhất cho thuộc tính \"fragment.index\" trong khoảng từ 0 đến giá trị của thuộc tính \"fragment.count\".");

    public static final AllowableValue DELIMITER_STRATEGY_FILENAME = new AllowableValue(
            "Tên tệp", "Tên tệp", "Các giá trị của Header, Footer và Demarcator sẽ được lấy từ nội dung của một tệp");
    public static final AllowableValue DELIMITER_STRATEGY_TEXT = new AllowableValue(
            "Văn bản", "Văn bản", "Các giá trị của Header, Footer và Demarcator sẽ được chỉ định làm giá trị thuộc tính");
    public static final AllowableValue DELIMITER_STRATEGY_NONE = new AllowableValue(
        "Không sử dụng dấu phân cách", "Không sử dụng dấu phân cách", "Sẽ không có Header, Footer hoặc Demarcator nào được sử dụng");

    public static final String MERGE_FORMAT_TAR_VALUE = "TAR";
    public static final String MERGE_FORMAT_ZIP_VALUE = "ZIP";
    public static final String MERGE_FORMAT_FLOWFILE_STREAM_V3_VALUE = "FlowFile Stream, v3";
    public static final String MERGE_FORMAT_FLOWFILE_STREAM_V2_VALUE = "FlowFile Stream, v2";
    public static final String MERGE_FORMAT_FLOWFILE_TAR_V1_VALUE = "FlowFile Tar, v1";
    public static final String MERGE_FORMAT_CONCAT_VALUE = "Nối nhị phân";
    public static final String MERGE_FORMAT_AVRO_VALUE = "Avro";

    public static final AllowableValue MERGE_FORMAT_TAR = new AllowableValue(
            MERGE_FORMAT_TAR_VALUE,
            MERGE_FORMAT_TAR_VALUE,
            "Một bin FlowFile sẽ được kết hợp thành một tệp TAR duy nhất. Thuộc tính <path> của FlowFile sẽ được sử dụng để tạo một thư mục trong tệp TAR "
            + "nếu thuộc tính <Giữ đường dẫn> được đặt thành true; nếu không, tất cả các FlowFile sẽ được thêm vào thư mục gốc của tệp TAR. "
            + "Nếu một FlowFile có thuộc tính tên là <tar.permissions> gồm 3 ký tự, mỗi ký tự từ 0-7, thuộc tính đó sẽ được sử dụng "
            + "làm 'mode' của mục TAR.");
    public static final AllowableValue MERGE_FORMAT_ZIP = new AllowableValue(
            MERGE_FORMAT_ZIP_VALUE,
            MERGE_FORMAT_ZIP_VALUE,
            "Một bin FlowFile sẽ được kết hợp thành một tệp ZIP duy nhất. Thuộc tính <path> của FlowFile sẽ được sử dụng để tạo một thư mục trong tệp ZIP "
            + "nếu thuộc tính <Giữ đường dẫn> được đặt thành true; nếu không, tất cả các FlowFile sẽ được thêm vào thư mục gốc của tệp ZIP. "
            + "Thuộc tính <Mức nén> cho biết mức nén ZIP sẽ sử dụng.");
    public static final AllowableValue MERGE_FORMAT_FLOWFILE_STREAM_V3 = new AllowableValue(
            MERGE_FORMAT_FLOWFILE_STREAM_V3_VALUE,
            MERGE_FORMAT_FLOWFILE_STREAM_V3_VALUE,
            "Một bin FlowFile sẽ được kết hợp thành một Luồng FlowFile Phiên bản 3 duy nhất");
    public static final AllowableValue MERGE_FORMAT_FLOWFILE_STREAM_V2 = new AllowableValue(
            MERGE_FORMAT_FLOWFILE_STREAM_V2_VALUE,
            MERGE_FORMAT_FLOWFILE_STREAM_V2_VALUE,
            "Một bin FlowFile sẽ được kết hợp thành một Luồng FlowFile Phiên bản 2 duy nhất");
    public static final AllowableValue MERGE_FORMAT_FLOWFILE_TAR_V1 = new AllowableValue(
            MERGE_FORMAT_FLOWFILE_TAR_V1_VALUE,
            MERGE_FORMAT_FLOWFILE_TAR_V1_VALUE,
            "Một bin FlowFile sẽ được kết hợp thành một Gói FlowFile Phiên bản 1 duy nhất");
    public static final AllowableValue MERGE_FORMAT_CONCAT = new AllowableValue(
            MERGE_FORMAT_CONCAT_VALUE,
            MERGE_FORMAT_CONCAT_VALUE,
            "Nội dung của tất cả các FlowFile sẽ được nối lại với nhau thành một FlowFile duy nhất");
    public static final AllowableValue MERGE_FORMAT_AVRO = new AllowableValue(
            MERGE_FORMAT_AVRO_VALUE,
            MERGE_FORMAT_AVRO_VALUE,
            "Nội dung Avro của tất cả các FlowFile sẽ được nối lại với nhau thành một FlowFile duy nhất");


    public static final String TAR_PERMISSIONS_ATTRIBUTE = "tar.permissions";
    public static final String MERGE_COUNT_ATTRIBUTE = "merge.count";
    public static final String MERGE_BIN_AGE_ATTRIBUTE = "merge.bin.age";
    public static final String MERGE_UUID_ATTRIBUTE = "merge.uuid";
    public static final String REASON_FOR_MERGING = "merge.reason";

    public static final PropertyDescriptor MERGE_STRATEGY = new PropertyDescriptor.Builder()
            .name("Chiến lược hợp nhất")
            .description("Chỉ định thuật toán được sử dụng để hợp nhất nội dung. Thuật toán 'Chống phân mảnh' kết hợp các mảnh được liên kết bởi "
                    + "các thuộc tính trở lại thành một FlowFile gắn kết duy nhất. 'Thuật toán Đóng gói Bin' tạo ra một FlowFile được điền bởi các "
                    + "FlowFile được chọn tùy ý")
            .required(true)
            .allowableValues(MERGE_STRATEGY_BIN_PACK, MERGE_STRATEGY_DEFRAGMENT)
            .defaultValue(MERGE_STRATEGY_BIN_PACK.getValue())
            .build();
    public static final PropertyDescriptor MERGE_FORMAT = new PropertyDescriptor.Builder()
            .required(true)
            .name("Định dạng hợp nhất")
            .description("Xác định định dạng sẽ được sử dụng để hợp nhất nội dung.")
            .allowableValues(MERGE_FORMAT_TAR, MERGE_FORMAT_ZIP, MERGE_FORMAT_FLOWFILE_STREAM_V3, MERGE_FORMAT_FLOWFILE_STREAM_V2, MERGE_FORMAT_FLOWFILE_TAR_V1, MERGE_FORMAT_CONCAT, MERGE_FORMAT_AVRO)
            .defaultValue(MERGE_FORMAT_CONCAT.getValue())
            .build();

    public static final PropertyDescriptor METADATA_STRATEGY = new PropertyDescriptor.Builder()
        .required(true)
        .name("mergecontent-metadata-strategy")
        .displayName("Chiến lược siêu dữ liệu")
        .description("Đối với các FlowFile có định dạng đầu vào hỗ trợ siêu dữ liệu (ví dụ: Avro), thuộc tính này xác định siêu dữ liệu nào sẽ được thêm vào gói. "
            + "Nếu 'Sử dụng siêu dữ liệu đầu tiên' được chọn, các khóa/giá trị siêu dữ liệu từ FlowFile đầu tiên được đóng gói sẽ được sử dụng. Nếu 'Chỉ giữ lại siêu dữ liệu chung' được chọn, "
            + "chỉ siêu dữ liệu tồn tại trên tất cả các FlowFile trong gói, với cùng một giá trị, sẽ được giữ lại. Nếu 'Bỏ qua siêu dữ liệu' được chọn, không có siêu dữ liệu nào được chuyển đến "
            + "FlowFile được đóng gói đầu ra. Nếu 'Không hợp nhất siêu dữ liệu không phổ biến' được chọn, bất kỳ FlowFile nào có giá trị siêu dữ liệu không khớp với giá trị của FlowFile được đóng gói đầu tiên "
            + "sẽ không được hợp nhất.")
        .allowableValues(METADATA_STRATEGY_USE_FIRST, METADATA_STRATEGY_ALL_COMMON, METADATA_STRATEGY_DO_NOT_MERGE, METADATA_STRATEGY_IGNORE)
        .defaultValue(METADATA_STRATEGY_DO_NOT_MERGE.getValue())
        .dependsOn(MERGE_FORMAT, MERGE_FORMAT_AVRO)
        .build();

    public static final PropertyDescriptor CORRELATION_ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
            .name("Tên thuộc tính tương quan")
            .description("Nếu được chỉ định, các FlowFile giống nhau sẽ được gom vào cùng một bin, trong đó 'FlowFile giống nhau' có nghĩa là các FlowFile có cùng giá trị cho "
                    + "Thuộc tính này. Nếu không được chỉ định, các FlowFile được đóng gói theo thứ tự chúng được lấy ra khỏi hàng đợi.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.ATTRIBUTE_KEY_VALIDATOR)
            .defaultValue(null)
            .dependsOn(MERGE_STRATEGY, MERGE_STRATEGY_BIN_PACK)
            .build();

    public static final PropertyDescriptor DELIMITER_STRATEGY = new PropertyDescriptor.Builder()
            .required(true)
            .name("Chiến lược phân cách")
            .description("Xác định xem Header, Footer và Demarcator có nên trỏ đến các tệp chứa nội dung tương ứng hay không, hoặc liệu "
                    + "các giá trị của các thuộc tính có nên được sử dụng làm nội dung hay không.")
            .allowableValues(DELIMITER_STRATEGY_NONE, DELIMITER_STRATEGY_FILENAME, DELIMITER_STRATEGY_TEXT)
            .defaultValue(DELIMITER_STRATEGY_NONE.getValue())
            .dependsOn(MERGE_FORMAT, MERGE_FORMAT_CONCAT_VALUE)
            .build();
    public static final PropertyDescriptor HEADER = new PropertyDescriptor.Builder()
            .name("Tệp tiêu đề")
            .displayName("Tiêu đề")
            .description("Tên tệp hoặc văn bản chỉ định tiêu đề sẽ sử dụng. Nếu không được chỉ định, sẽ không có tiêu đề nào được cung cấp.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .dependsOn(DELIMITER_STRATEGY, DELIMITER_STRATEGY_FILENAME, DELIMITER_STRATEGY_TEXT)
            .dependsOn(MERGE_FORMAT, MERGE_FORMAT_CONCAT)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE, ResourceType.TEXT)
            .build();
    public static final PropertyDescriptor FOOTER = new PropertyDescriptor.Builder()
            .name("Tệp chân trang")
            .displayName("Chân trang")
            .description("Tên tệp hoặc văn bản chỉ định chân trang sẽ sử dụng. Nếu không được chỉ định, sẽ không có chân trang nào được cung cấp.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .dependsOn(DELIMITER_STRATEGY, DELIMITER_STRATEGY_FILENAME, DELIMITER_STRATEGY_TEXT)
            .dependsOn(MERGE_FORMAT, MERGE_FORMAT_CONCAT)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE, ResourceType.TEXT)
            .build();
    public static final PropertyDescriptor DEMARCATOR = new PropertyDescriptor.Builder()
            .name("Tệp phân cách")
            .displayName("Phân cách")
            .description("Tên tệp hoặc văn bản chỉ định dấu phân cách sẽ sử dụng. Nếu không được chỉ định, sẽ không có dấu phân cách nào được cung cấp.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .dependsOn(DELIMITER_STRATEGY, DELIMITER_STRATEGY_FILENAME, DELIMITER_STRATEGY_TEXT)
            .dependsOn(MERGE_FORMAT, MERGE_FORMAT_CONCAT)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE, ResourceType.TEXT)
            .build();
    public static final PropertyDescriptor COMPRESSION_LEVEL = new PropertyDescriptor.Builder()
            .name("Mức nén")
            .description("Chỉ định mức nén sẽ sử dụng khi sử dụng Định dạng hợp nhất Zip; nếu không sử dụng Định dạng hợp nhất Zip, giá trị này sẽ "
                    + "bị bỏ qua")
            .required(true)
            .allowableValues("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
            .defaultValue("1")
            .dependsOn(MERGE_FORMAT, MERGE_FORMAT_ZIP)
            .build();
    public static final PropertyDescriptor KEEP_PATH = new PropertyDescriptor.Builder()
            .name("Giữ đường dẫn")
            .description("Nếu sử dụng Định dạng hợp nhất Zip hoặc Tar, chỉ định liệu đường dẫn của các FlowFile có nên được bao gồm trong tên mục của chúng hay không.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .dependsOn(MERGE_FORMAT, MERGE_FORMAT_TAR, MERGE_FORMAT_ZIP)
            .build();
    public static final PropertyDescriptor TAR_MODIFIED_TIME = new PropertyDescriptor.Builder()
            .name("Thời gian sửa đổi Tar")
            .description("Nếu sử dụng Định dạng hợp nhất Tar, chỉ định xem mục nhập Tar có nên lưu trữ dấu thời gian đã sửa đổi bằng biểu thức "
                    + "(ví dụ: ${file.lastModifiedTime}) hoặc giá trị tĩnh, cả hai đều phải khớp với định dạng ISO8601 'yyyy-MM-dd'T'HH:mm:ssZ'.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("${file.lastModifiedTime}")
            .dependsOn(MERGE_FORMAT, MERGE_FORMAT_TAR)
            .build();

    public static final Relationship REL_MERGED = new Relationship.Builder().name("merged").description("The FlowFile containing the merged content").build();

    public static final Pattern NUMBER_PATTERN = Pattern.compile("\\d+");

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_ORIGINAL);
        relationships.add(REL_FAILURE);
        relationships.add(REL_MERGED);
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(MERGE_STRATEGY);
        descriptors.add(MERGE_FORMAT);
        descriptors.add(AttributeStrategyUtil.ATTRIBUTE_STRATEGY);
        descriptors.add(CORRELATION_ATTRIBUTE_NAME);
        descriptors.add(METADATA_STRATEGY);
        descriptors.add(addBinPackingDependency(MIN_ENTRIES));
        descriptors.add(addBinPackingDependency(MAX_ENTRIES));
        descriptors.add(addBinPackingDependency(MIN_SIZE));
        descriptors.add(addBinPackingDependency(MAX_SIZE));
        descriptors.add(MAX_BIN_AGE);
        descriptors.add(MAX_BIN_COUNT);
        descriptors.add(DELIMITER_STRATEGY);
        descriptors.add(HEADER);
        descriptors.add(FOOTER);
        descriptors.add(DEMARCATOR);
        descriptors.add(COMPRESSION_LEVEL);
        descriptors.add(KEEP_PATH);
        descriptors.add(TAR_MODIFIED_TIME);
        return descriptors;
    }

    // Convenience method to make creation of property descriptors cleaner
    private PropertyDescriptor addBinPackingDependency(final PropertyDescriptor original) {
        return new PropertyDescriptor.Builder().fromPropertyDescriptor(original).dependsOn(MERGE_STRATEGY, MERGE_STRATEGY_BIN_PACK).build();
    }

    @Override
    protected Collection<ValidationResult> additionalCustomValidation(ValidationContext context) {
        final Collection<ValidationResult> results = new ArrayList<>();

        final String delimiterStrategy = context.getProperty(DELIMITER_STRATEGY).getValue();
        final String mergeFormat = context.getProperty(MERGE_FORMAT).getValue();
        if (DELIMITER_STRATEGY_FILENAME.getValue().equals(delimiterStrategy) && MERGE_FORMAT_CONCAT.getValue().equals(mergeFormat)) {
            final String headerValue = context.getProperty(HEADER).getValue();
            if (headerValue != null) {
                results.add(StandardValidators.FILE_EXISTS_VALIDATOR.validate(HEADER.getName(), headerValue, context));
            }

            final String footerValue = context.getProperty(FOOTER).getValue();
            if (footerValue != null) {
                results.add(StandardValidators.FILE_EXISTS_VALIDATOR.validate(FOOTER.getName(), footerValue, context));
            }

            final String demarcatorValue = context.getProperty(DEMARCATOR).getValue();
            if (demarcatorValue != null) {
                results.add(StandardValidators.FILE_EXISTS_VALIDATOR.validate(DEMARCATOR.getName(), demarcatorValue, context));
            }
        }
        return results;
    }

    private byte[] readContent(final String filename) throws IOException {
        return Files.readAllBytes(Paths.get(filename));
    }

    @Override
    protected FlowFile preprocessFlowFile(final ProcessContext context, final ProcessSession session, final FlowFile flowFile) {
        FlowFile processed = flowFile;
        // handle backward compatibility with old segment attributes
        if (processed.getAttribute(FRAGMENT_COUNT_ATTRIBUTE) == null && processed.getAttribute(SEGMENT_COUNT_ATTRIBUTE) != null) {
            processed = session.putAttribute(processed, FRAGMENT_COUNT_ATTRIBUTE, processed.getAttribute(SEGMENT_COUNT_ATTRIBUTE));
        }
        if (processed.getAttribute(FRAGMENT_INDEX_ATTRIBUTE) == null && processed.getAttribute(SEGMENT_INDEX_ATTRIBUTE) != null) {
            processed = session.putAttribute(processed, FRAGMENT_INDEX_ATTRIBUTE, processed.getAttribute(SEGMENT_INDEX_ATTRIBUTE));
        }
        if (processed.getAttribute(FRAGMENT_ID_ATTRIBUTE) == null && processed.getAttribute(SEGMENT_ID_ATTRIBUTE) != null) {
            processed = session.putAttribute(processed, FRAGMENT_ID_ATTRIBUTE, processed.getAttribute(SEGMENT_ID_ATTRIBUTE));
        }

        return processed;
    }

    @Override
    protected String getGroupId(final ProcessContext context, final FlowFile flowFile, final ProcessSession session) {
        final String correlationAttributeName = context.getProperty(CORRELATION_ATTRIBUTE_NAME)
                .evaluateAttributeExpressions(flowFile).getValue();
        String groupId = correlationAttributeName == null ? null : flowFile.getAttribute(correlationAttributeName);

        // when MERGE_STRATEGY is Defragment and correlationAttributeName is null then bin by fragment.identifier
        if (groupId == null && MERGE_STRATEGY_DEFRAGMENT.getValue().equals(context.getProperty(MERGE_STRATEGY).getValue())) {
            groupId = flowFile.getAttribute(FRAGMENT_ID_ATTRIBUTE);
        }

        return groupId;
    }

    @Override
    protected void setUpBinManager(final BinManager binManager, final ProcessContext context) {
        if (MERGE_STRATEGY_DEFRAGMENT.getValue().equals(context.getProperty(MERGE_STRATEGY).getValue())) {
            binManager.setFileCountAttribute(FRAGMENT_COUNT_ATTRIBUTE);
        } else {
            binManager.setFileCountAttribute(null);
        }
    }

    @Override
    protected BinProcessingResult processBin(final Bin bin, final ProcessContext context) throws ProcessException {
        final BinProcessingResult binProcessingResult = new BinProcessingResult(true);
        final String mergeFormat = context.getProperty(MERGE_FORMAT).getValue();
        MergeBin merger;
        switch (mergeFormat) {
            case MERGE_FORMAT_TAR_VALUE:
                merger = new TarMerge();
                break;
            case MERGE_FORMAT_ZIP_VALUE:
                merger = new ZipMerge(context.getProperty(COMPRESSION_LEVEL).asInteger());
                break;
            case MERGE_FORMAT_FLOWFILE_STREAM_V3_VALUE:
                merger = new FlowFileStreamMerger(new FlowFilePackagerV3(), StandardFlowFileMediaType.VERSION_3.getMediaType());
                break;
            case MERGE_FORMAT_FLOWFILE_STREAM_V2_VALUE:
                merger = new FlowFileStreamMerger(new FlowFilePackagerV2(), StandardFlowFileMediaType.VERSION_2.getMediaType());
                break;
            case MERGE_FORMAT_FLOWFILE_TAR_V1_VALUE:
                merger = new FlowFileStreamMerger(new FlowFilePackagerV1(), StandardFlowFileMediaType.VERSION_1.getMediaType());
                break;
            case MERGE_FORMAT_CONCAT_VALUE:
                merger = new BinaryConcatenationMerge();
                break;
            case MERGE_FORMAT_AVRO_VALUE:
                merger = new AvroMerge();
                break;
            default:
                throw new AssertionError();
        }

        final AttributeStrategy attributeStrategy = AttributeStrategyUtil.strategyFor(context);

        final List<FlowFile> contents = bin.getContents();
        final ProcessSession binSession = bin.getSession();

        if (MERGE_STRATEGY_DEFRAGMENT.getValue().equals(context.getProperty(MERGE_STRATEGY).getValue())) {
            final String error = getDefragmentValidationError(bin.getContents());

            // Fail the flow files and commit them
            if (error != null) {
                final String binDescription = contents.size() <= 10 ? contents.toString() : contents.size() + " FlowFiles";
                getLogger().error("{}; routing {} to failure", error, binDescription);
                binSession.transfer(contents, REL_FAILURE);
                binSession.commitAsync();

                return binProcessingResult;
            }

            Collections.sort(contents, new FragmentComparator());
        }

        FlowFile bundle = merger.merge(bin, context);

        // keep the filename, as it is added to the bundle.
        final String filename = bundle.getAttribute(CoreAttributes.FILENAME.key());

        // merge all of the attributes
        final Map<String, String> bundleAttributes = attributeStrategy.getMergedAttributes(contents);
        bundleAttributes.put(CoreAttributes.MIME_TYPE.key(), merger.getMergedContentType());
        // restore the filename of the bundle
        bundleAttributes.put(CoreAttributes.FILENAME.key(), filename);
        bundleAttributes.put(MERGE_COUNT_ATTRIBUTE, Integer.toString(contents.size()));
        bundleAttributes.put(MERGE_BIN_AGE_ATTRIBUTE, Long.toString(bin.getBinAge()));

        bundleAttributes.put(REASON_FOR_MERGING, bin.getEvictionReason().name());

        bundle = binSession.putAllAttributes(bundle, bundleAttributes);

        final String inputDescription = contents.size() < 10 ? contents.toString() : contents.size() + " FlowFiles";

        getLogger().info("Merged {} into {}. Reason for merging: {}", inputDescription, bundle, bin.getEvictionReason());

        binSession.transfer(bundle, REL_MERGED);
        binProcessingResult.getAttributes().put(MERGE_UUID_ATTRIBUTE, bundle.getAttribute(CoreAttributes.UUID.key()));

        for (final FlowFile unmerged : merger.getUnmergedFlowFiles()) {
            final FlowFile unmergedCopy = binSession.clone(unmerged);
            binSession.transfer(unmergedCopy, REL_FAILURE);
        }

        // We haven't committed anything, parent will take care of it
        binProcessingResult.setCommitted(false);
        return binProcessingResult;
    }

    private String getDefragmentValidationError(final List<FlowFile> binContents) {
        if (binContents.isEmpty()) {
            return null;
        }

        // If we are defragmenting, all fragments must have the appropriate attributes.
        String decidedFragmentCount = null;
        String fragmentIdentifier = null;
        for (final FlowFile flowFile : binContents) {
            final String fragmentIndex = flowFile.getAttribute(FRAGMENT_INDEX_ATTRIBUTE);
            if (!isNumber(fragmentIndex)) {
                return "Cannot Defragment " + flowFile + " because it does not have an integer value for the " + FRAGMENT_INDEX_ATTRIBUTE + " attribute";
            }

            fragmentIdentifier = flowFile.getAttribute(FRAGMENT_ID_ATTRIBUTE);

            final String fragmentCount = flowFile.getAttribute(FRAGMENT_COUNT_ATTRIBUTE);
            if (fragmentCount != null) {
                if (!isNumber(fragmentCount)) {
                    return "Cannot Defragment " + flowFile + " because it does not have an integer value for the " + FRAGMENT_COUNT_ATTRIBUTE + " attribute";
                } else if (decidedFragmentCount == null) {
                    decidedFragmentCount = fragmentCount;
                } else if (!decidedFragmentCount.equals(fragmentCount)) {
                    return "Cannot Defragment " + flowFile + " because it is grouped with another FlowFile, and the two have differing values for the "
                            + FRAGMENT_COUNT_ATTRIBUTE + " attribute: " + decidedFragmentCount + " and " + fragmentCount;
                }
            }
        }

        if (decidedFragmentCount == null) {
            return "Cannot Defragment FlowFiles with Fragment Identifier " + fragmentIdentifier + " because no FlowFile arrived with the " + FRAGMENT_COUNT_ATTRIBUTE + " attribute "
                + "and the expected number of fragments is unknown";
        }

        final int numericFragmentCount;
        try {
            numericFragmentCount = Integer.parseInt(decidedFragmentCount);
        } catch (final NumberFormatException nfe) {
            return "Cannot Defragment FlowFiles with Fragment Identifier " + fragmentIdentifier + " because the " + FRAGMENT_COUNT_ATTRIBUTE + " has a non-integer value of " + decidedFragmentCount;
        }

        if (binContents.size() < numericFragmentCount) {
            return "Cannot Defragment FlowFiles with Fragment Identifier " + fragmentIdentifier + " because the expected number of fragments is " + decidedFragmentCount + " but found only "
                + binContents.size() + " fragments";
        }

        if (binContents.size() > numericFragmentCount) {
            return "Cannot Defragment FlowFiles with Fragment Identifier " + fragmentIdentifier + " because the expected number of fragments is " + decidedFragmentCount + " but found "
                + binContents.size() + " fragments for this identifier";
        }

        return null;
    }

    private boolean isNumber(final String value) {
        if (value == null) {
            return false;
        }

        return NUMBER_PATTERN.matcher(value).matches();
    }

    private void removeFlowFileFromSession(final ProcessSession session, final FlowFile flowFile, final ProcessContext context) {
        try {
            session.remove(flowFile);
        } catch (final Exception e) {
            getLogger().error("Failed to remove merged FlowFile from the session after merge failure during \""
                    + context.getProperty(MERGE_FORMAT).getValue() + "\" merge.", e);
        }
    }

    private class BinaryConcatenationMerge implements MergeBin {

        private String mimeType = "application/octet-stream";

        public BinaryConcatenationMerge() {
        }

        @Override
        public FlowFile merge(final Bin bin, final ProcessContext context) {
            final List<FlowFile> contents = bin.getContents();

            final ProcessSession session = bin.getSession();
            FlowFile bundle = session.create(bin.getContents());
            final AtomicReference<String> bundleMimeTypeRef = new AtomicReference<>(null);
            try {
                bundle = session.write(bundle, new OutputStreamCallback() {
                    @Override
                    public void process(final OutputStream out) throws IOException {
                        final byte[] header = getDelimiterContent(context, contents, HEADER);
                        if (header != null) {
                            out.write(header);
                        }

                        final byte[] demarcator = getDelimiterContent(context, contents, DEMARCATOR);

                        boolean isFirst = true;
                        final Iterator<FlowFile> itr = contents.iterator();
                        while (itr.hasNext()) {
                            final FlowFile flowFile = itr.next();
                            bin.getSession().read(flowFile, in -> StreamUtils.copy(in, out));

                            if (itr.hasNext()) {
                                if (demarcator != null) {
                                    out.write(demarcator);
                                }
                            }

                            final String flowFileMimeType = flowFile.getAttribute(CoreAttributes.MIME_TYPE.key());
                            if (isFirst) {
                                bundleMimeTypeRef.set(flowFileMimeType);
                                isFirst = false;
                            } else {
                                if (bundleMimeTypeRef.get() != null && !bundleMimeTypeRef.get().equals(flowFileMimeType)) {
                                    bundleMimeTypeRef.set(null);
                                }
                            }
                        }

                        final byte[] footer = getDelimiterContent(context, contents, FOOTER);
                        if (footer != null) {
                            out.write(footer);
                        }
                    }
                });
            } catch (final Exception e) {
                removeFlowFileFromSession(session, bundle, context);
                throw e;
            }

            session.getProvenanceReporter().join(contents, bundle);
            bundle = session.putAttribute(bundle, CoreAttributes.FILENAME.key(), createFilename(contents));
            if (bundleMimeTypeRef.get() != null) {
                this.mimeType = bundleMimeTypeRef.get();
            }

            return bundle;
        }

        private byte[] getDelimiterContent(final ProcessContext context, final List<FlowFile> wrappers, final PropertyDescriptor descriptor) throws IOException {
            final String delimiterStrategyValue = context.getProperty(DELIMITER_STRATEGY).getValue();
            if (DELIMITER_STRATEGY_FILENAME.getValue().equals(delimiterStrategyValue)) {
                return getDelimiterFileContent(context, wrappers, descriptor);
            } else if (DELIMITER_STRATEGY_TEXT.getValue().equals(delimiterStrategyValue)) {
                return getDelimiterTextContent(context, wrappers, descriptor);
            } else {
                return null;
            }
        }

        private byte[] getDelimiterFileContent(final ProcessContext context, final List<FlowFile> flowFiles, final PropertyDescriptor descriptor)
                throws IOException {
            byte[] property = null;
            if (flowFiles != null && flowFiles.size() > 0) {
                final FlowFile flowFile = flowFiles.get(0);
                if (flowFile != null) {
                    final String value = context.getProperty(descriptor).evaluateAttributeExpressions(flowFile).getValue();
                    if (value != null) {
                        property = readContent(value);
                    }
                }
            }
            return property;
        }

        private byte[] getDelimiterTextContent(final ProcessContext context, final List<FlowFile> flowFiles, final PropertyDescriptor descriptor) {
            byte[] property = null;
            if (flowFiles != null && flowFiles.size() > 0) {
                final FlowFile flowFile = flowFiles.get(0);
                if (flowFile != null) {
                    final String value = context.getProperty(descriptor).evaluateAttributeExpressions(flowFile).getValue();
                    if (value != null) {
                        property = value.getBytes(StandardCharsets.UTF_8);
                    }
                }
            }
            return property;
        }

        @Override
        public String getMergedContentType() {
            return mimeType;
        }

        @Override
        public List<FlowFile> getUnmergedFlowFiles() {
            return Collections.emptyList();
        }
    }


    private String getPath(final FlowFile flowFile) {
        Path path = Paths.get(flowFile.getAttribute(CoreAttributes.PATH.key()));
        if (path.getNameCount() == 0) {
            return "";
        }

        if (".".equals(path.getName(0).toString())) {
            path = path.getNameCount() == 1 ? null : path.subpath(1, path.getNameCount());
        }

        return path == null ? "" : path.toString() + "/";
    }

    private String createFilename(final List<FlowFile> flowFiles) {
        if (flowFiles.size() == 1) {
            return flowFiles.get(0).getAttribute(CoreAttributes.FILENAME.key());
        } else {
            final FlowFile ff = flowFiles.get(0);
            final String origFilename = ff.getAttribute(SEGMENT_ORIGINAL_FILENAME);
            if (origFilename != null) {
                return origFilename;
            } else {
                return String.valueOf(System.nanoTime());
            }
        }
    }

    private class TarMerge implements MergeBin {

        @Override
        public FlowFile merge(final Bin bin, final ProcessContext context) {
            final List<FlowFile> contents = bin.getContents();
            final ProcessSession session = bin.getSession();
            final boolean keepPath = context.getProperty(KEEP_PATH).asBoolean();
            FlowFile bundle = session.create(); // we don't pass the parents to the #create method because the parents belong to different sessions

            try {
                bundle = session.putAttribute(bundle, CoreAttributes.FILENAME.key(), createFilename(contents) + ".tar");
                bundle = session.write(bundle, new OutputStreamCallback() {
                    @Override
                    public void process(final OutputStream rawOut) throws IOException {
                        try (final OutputStream bufferedOut = new BufferedOutputStream(rawOut);
                            final TarArchiveOutputStream out = new TarArchiveOutputStream(bufferedOut)) {

                            out.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
                            // if any one of the FlowFiles is larger than the default maximum tar entry size, then we set bigNumberMode to handle it
                            if (getMaxEntrySize(contents) >= TarConstants.MAXSIZE) {
                                out.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_POSIX);
                            }
                            for (final FlowFile flowFile : contents) {
                                final String path = keepPath ? getPath(flowFile) : "";
                                final String entryName = path + flowFile.getAttribute(CoreAttributes.FILENAME.key());

                                final TarArchiveEntry tarEntry = new TarArchiveEntry(entryName);
                                tarEntry.setSize(flowFile.getSize());
                                final String permissionsVal = flowFile.getAttribute(TAR_PERMISSIONS_ATTRIBUTE);
                                if (permissionsVal != null) {
                                    try {
                                        tarEntry.setMode(Integer.parseInt(permissionsVal));
                                    } catch (final Exception e) {
                                        getLogger().debug("Attribute {} of {} is set to {}; expected 3 digits between 0-7, so ignoring", TAR_PERMISSIONS_ATTRIBUTE, flowFile, permissionsVal);
                                    }
                                }

                                final String modTime = context.getProperty(TAR_MODIFIED_TIME)
                                    .evaluateAttributeExpressions(flowFile).getValue();
                                if (StringUtils.isNotBlank(modTime)) {
                                    try {
                                        tarEntry.setModTime(Instant.parse(modTime).toEpochMilli());
                                    } catch (final Exception e) {
                                        getLogger().debug("Attribute {} of {} is set to {}; expected ISO8601 format, so ignoring", TAR_MODIFIED_TIME, flowFile, modTime);
                                    }
                                }

                                out.putArchiveEntry(tarEntry);

                                bin.getSession().exportTo(flowFile, out);
                                out.closeArchiveEntry();
                            }
                        }
                    }
                });
            } catch (final Exception e) {
                removeFlowFileFromSession(session, bundle, context);
                throw e;
            }

            bin.getSession().getProvenanceReporter().join(contents, bundle);
            return bundle;
        }

        private long getMaxEntrySize(final List<FlowFile> contents) {
            final OptionalLong maxSize = contents.stream()
                .parallel()
                .mapToLong(ff -> ff.getSize())
                .max();
            return maxSize.orElse(0L);
        }

        @Override
        public String getMergedContentType() {
            return "application/tar";
        }

        @Override
        public List<FlowFile> getUnmergedFlowFiles() {
            return Collections.emptyList();
        }
    }

    private class FlowFileStreamMerger implements MergeBin {

        private final FlowFilePackager packager;
        private final String mimeType;

        public FlowFileStreamMerger(final FlowFilePackager packager, final String mimeType) {
            this.packager = packager;
            this.mimeType = mimeType;
        }

        @Override
        public FlowFile merge(final Bin bin, final ProcessContext context) {
            final ProcessSession session = bin.getSession();
            final List<FlowFile> contents = bin.getContents();

            FlowFile bundle = session.create(contents);

            try {
                bundle = session.write(bundle, new OutputStreamCallback() {
                    @Override
                    public void process(final OutputStream rawOut) throws IOException {
                        try (final OutputStream bufferedOut = new BufferedOutputStream(rawOut)) {
                            // we don't want the packager closing the stream. V1 creates a TAR Output Stream, which then gets
                            // closed, which in turn closes the underlying OutputStream, and we want to protect ourselves against that.
                            final OutputStream out = new NonCloseableOutputStream(bufferedOut);

                            for (final FlowFile flowFile : contents) {
                                bin.getSession().read(flowFile, new InputStreamCallback() {
                                    @Override
                                    public void process(final InputStream rawIn) throws IOException {
                                        try (final InputStream in = new BufferedInputStream(rawIn)) {
                                            final Map<String, String> attributes = new HashMap<>(flowFile.getAttributes());

                                            // for backward compatibility purposes, we add the "legacy" Life attributes
                                            attributes.put("nf.file.name", attributes.get(CoreAttributes.FILENAME.key()));
                                            attributes.put("nf.file.path", attributes.get(CoreAttributes.PATH.key()));
                                            if (attributes.containsKey(CoreAttributes.MIME_TYPE.key())) {
                                                attributes.put("content-type", attributes.get(CoreAttributes.MIME_TYPE.key()));
                                            }
                                            packager.packageFlowFile(in, out, attributes, flowFile.getSize());
                                        }
                                    }
                                });
                            }
                        }
                    }
                });
            } catch (final Exception e) {
                removeFlowFileFromSession(session, bundle, context);
                throw e;
            }

            bundle = session.putAttribute(bundle, CoreAttributes.FILENAME.key(), createFilename(contents) + ".pkg");
            session.getProvenanceReporter().join(contents, bundle);
            return bundle;
        }

        @Override
        public String getMergedContentType() {
            return mimeType;
        }

        @Override
        public List<FlowFile> getUnmergedFlowFiles() {
            return Collections.emptyList();
        }
    }

    private class ZipMerge implements MergeBin {

        private final int compressionLevel;

        private final List<FlowFile> unmerged = new ArrayList<>();

        public ZipMerge(final int compressionLevel) {
            this.compressionLevel = compressionLevel;
        }

        @Override
        public FlowFile merge(final Bin bin, final ProcessContext context) {
            final boolean keepPath = context.getProperty(KEEP_PATH).asBoolean();

            final ProcessSession session = bin.getSession();
            final List<FlowFile> contents = bin.getContents();
            unmerged.addAll(contents);

            FlowFile bundle = session.create(contents);

            try {
                bundle = session.putAttribute(bundle, CoreAttributes.FILENAME.key(), createFilename(contents) + ".zip");
                bundle = session.write(bundle, new OutputStreamCallback() {
                    @Override
                    public void process(final OutputStream rawOut) throws IOException {
                        try (final OutputStream bufferedOut = new BufferedOutputStream(rawOut);
                            final ZipOutputStream out = new ZipOutputStream(bufferedOut)) {
                            out.setLevel(compressionLevel);
                            for (final FlowFile flowFile : contents) {
                                final String path = keepPath ? getPath(flowFile) : "";
                                final String entryName = path + flowFile.getAttribute(CoreAttributes.FILENAME.key());
                                final ZipEntry zipEntry = new ZipEntry(entryName);
                                zipEntry.setSize(flowFile.getSize());
                                try {
                                    out.putNextEntry(zipEntry);

                                    bin.getSession().exportTo(flowFile, out);
                                    out.closeEntry();
                                    unmerged.remove(flowFile);
                                } catch (ZipException e) {
                                    getLogger().error("Encountered exception merging {}", flowFile, e);
                                }
                            }

                            out.finish();
                            out.flush();
                        }
                    }
                });
            } catch (final Exception e) {
                removeFlowFileFromSession(session, bundle, context);
                throw e;
            }

            session.getProvenanceReporter().join(contents, bundle);
            return bundle;
        }

        @Override
        public String getMergedContentType() {
            return "application/zip";
        }

        @Override
        public List<FlowFile> getUnmergedFlowFiles() {
            return unmerged;
        }
    }

    private class AvroMerge implements MergeBin {

        private final List<FlowFile> unmerged = new ArrayList<>();

        @Override
        public FlowFile merge(final Bin bin, final ProcessContext context) {
            final ProcessSession session = bin.getSession();
            final List<FlowFile> contents = bin.getContents();

            final String metadataStrategy = context.getProperty(METADATA_STRATEGY).getValue();
            final Map<String, byte[]> metadata = new TreeMap<>();
            final AtomicReference<Schema> schema = new AtomicReference<>(null);
            final AtomicReference<String> inputCodec = new AtomicReference<>(null);
            final DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<GenericRecord>());

            // we don't pass the parents to the #create method because the parents belong to different sessions
            FlowFile bundle = session.create(contents);
            try {
                bundle = session.write(bundle, new OutputStreamCallback() {
                    @Override
                    public void process(final OutputStream rawOut) throws IOException {
                        try (final OutputStream out = new BufferedOutputStream(rawOut)) {
                            for (final FlowFile flowFile : contents) {
                                bin.getSession().read(flowFile, new InputStreamCallback() {
                                    @Override
                                    public void process(InputStream in) throws IOException {
                                        boolean canMerge = true;
                                        try (DataFileStream<GenericRecord> reader = new DataFileStream<>(in,
                                            new GenericDatumReader<GenericRecord>())) {
                                            if (schema.get() == null) {
                                                // this is the first file - set up the writer, and store the
                                                // Schema & metadata we'll use.
                                                schema.set(reader.getSchema());
                                                if (!METADATA_STRATEGY_IGNORE.getValue().equals(metadataStrategy)) {
                                                    for (String key : reader.getMetaKeys()) {
                                                        if (!DataFileWriter.isReservedMeta(key)) {
                                                            byte[] metadatum = reader.getMeta(key);
                                                            metadata.put(key, metadatum);
                                                            writer.setMeta(key, metadatum);
                                                        }
                                                    }
                                                }
                                                inputCodec.set(reader.getMetaString(DataFileConstants.CODEC));
                                                if (inputCodec.get() == null) {
                                                    inputCodec.set(DataFileConstants.NULL_CODEC);
                                                }
                                                writer.setCodec(CodecFactory.fromString(inputCodec.get()));
                                                writer.create(schema.get(), out);
                                            } else {
                                                // check that we're appending to the same schema
                                                if (!schema.get().equals(reader.getSchema())) {
                                                    getLogger().debug("Input file {} has different schema - {}, not merging", flowFile.getId(), reader.getSchema().getName());
                                                    canMerge = false;
                                                    unmerged.add(flowFile);
                                                }

                                                if (METADATA_STRATEGY_DO_NOT_MERGE.getValue().equals(metadataStrategy)
                                                    || METADATA_STRATEGY_ALL_COMMON.getValue().equals(metadataStrategy)) {
                                                    // check that we're appending to the same metadata
                                                    for (String key : reader.getMetaKeys()) {
                                                        if (!DataFileWriter.isReservedMeta(key)) {
                                                            byte[] metadatum = reader.getMeta(key);
                                                            byte[] writersMetadatum = metadata.get(key);
                                                            if (!Arrays.equals(metadatum, writersMetadatum)) {
                                                                // Ignore additional metadata if ALL_COMMON is the strategy, otherwise don't merge
                                                                if (!METADATA_STRATEGY_ALL_COMMON.getValue().equals(metadataStrategy) || writersMetadatum != null) {
                                                                    getLogger().debug("Input file {} has different non-reserved metadata, not merging", flowFile.getId());
                                                                    canMerge = false;
                                                                    unmerged.add(flowFile);
                                                                }
                                                            }
                                                        }
                                                    }
                                                } // else the metadata in the first FlowFile was either ignored or retained in the if-clause above

                                                // check that we're appending to the same codec
                                                String thisCodec = reader.getMetaString(DataFileConstants.CODEC);
                                                if (thisCodec == null) {
                                                    thisCodec = DataFileConstants.NULL_CODEC;
                                                }
                                                if (!inputCodec.get().equals(thisCodec)) {
                                                    getLogger().debug("Input file {} has different codec, not merging", flowFile.getId());
                                                    canMerge = false;
                                                    unmerged.add(flowFile);
                                                }
                                            }

                                            // write the Avro content from the current FlowFile to the merged OutputStream
                                            if (canMerge) {
                                                writer.appendAllFrom(reader, false);
                                            }
                                        }
                                    }
                                });
                            }
                            writer.flush();
                        } finally {
                            writer.close();
                        }
                    }
                });
            } catch (final Exception e) {
                removeFlowFileFromSession(session, bundle, context);
                throw e;
            }

            final Collection<FlowFile> parents;
            if (unmerged.isEmpty()) {
                parents = contents;
            } else {
                parents = new HashSet<>(contents);
                parents.removeAll(unmerged);
            }

            session.getProvenanceReporter().join(parents, bundle);
            return bundle;
        }

        @Override
        public String getMergedContentType() {
            return "application/avro-binary";
        }

        @Override
        public List<FlowFile> getUnmergedFlowFiles() {
            return unmerged;
        }
    }



    private static class FragmentComparator implements Comparator<FlowFile> {

        @Override
        public int compare(final FlowFile o1, final FlowFile o2) {
            final int fragmentIndex1 = Integer.parseInt(o1.getAttribute(FRAGMENT_INDEX_ATTRIBUTE));
            final int fragmentIndex2 = Integer.parseInt(o2.getAttribute(FRAGMENT_INDEX_ATTRIBUTE));
            return Integer.compare(fragmentIndex1, fragmentIndex2);
        }
    }

    private interface MergeBin {

        FlowFile merge(Bin bin, ProcessContext context);

        String getMergedContentType();

        List<FlowFile> getUnmergedFlowFiles();
    }

}
