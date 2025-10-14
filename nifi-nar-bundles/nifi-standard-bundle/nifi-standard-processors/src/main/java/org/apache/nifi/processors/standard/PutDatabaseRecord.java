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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.io.BaseEncoding;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.pattern.RollbackOnFailure;
import org.apache.nifi.processors.standard.db.ColumnDescription;
import org.apache.nifi.processors.standard.db.DatabaseAdapter;
import org.apache.nifi.processors.standard.db.TableSchema;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.validation.RecordPathValidator;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.BatchUpdateException;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLTransientException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;
import static org.apache.nifi.expression.ExpressionLanguageScope.NONE;
import static org.apache.nifi.expression.ExpressionLanguageScope.VARIABLE_REGISTRY;

@EventDriven
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"sql", "record", "jdbc", "put", "database", "update", "insert", "delete"})
@CapabilityDescription("Bộ xử lý PutDatabaseRecord sử dụng một RecordReader được chỉ định để đầu vào (có thể là nhiều) bản ghi từ một tệp luồng đến. Các bản ghi này được dịch sang các câu lệnh SQL "
        + "và được thực thi như một giao dịch duy nhất. Nếu có lỗi xảy ra, tệp luồng sẽ được định tuyến đến thất bại hoặc thử lại, và nếu các bản ghi được truyền thành công, "
        + "tệp luồng đến sẽ "
        + "được định tuyến đến thành công. Loại câu lệnh được thực thi bởi bộ xử lý được chỉ định thông qua thuộc tính Loại Câu lệnh, chấp nhận một số giá trị được mã hóa cứng như INSERT, UPDATE và DELETE, "
        + "cũng như 'Sử dụng thuộc tính statement.type', điều này khiến bộ xử lý lấy loại câu lệnh từ một thuộc tính tệp luồng. QUAN TRỌNG: Nếu Loại Câu lệnh là UPDATE, thì các "
        + "bản ghi đến không được thay đổi giá trị của các khóa chính (hoặc Khóa Cập nhật được chỉ định bởi người dùng). Nếu gặp các bản ghi như vậy, câu lệnh UPDATE được phát hành đến cơ sở dữ liệu có thể không làm gì "
        + "(nếu không tìm thấy các bản ghi hiện có với các giá trị khóa chính mới), hoặc có thể vô tình làm hỏng dữ liệu hiện có (bằng cách thay đổi các bản ghi mà các giá trị khóa chính mới của chúng "
        + "tồn tại).")
@ReadsAttribute(attribute = PutDatabaseRecord.STATEMENT_TYPE_ATTRIBUTE, description = "Nếu 'Sử dụng thuộc tính statement.type' được chọn cho thuộc tính Loại Câu lệnh, giá trị của thuộc tính này "
        + "sẽ được sử dụng để xác định loại câu lệnh (INSERT, UPDATE, DELETE, SQL, v.v.) để tạo và thực thi.")
@WritesAttribute(attribute = PutDatabaseRecord.PUT_DATABASE_RECORD_ERROR, description = "Nếu có lỗi xảy ra trong quá trình xử lý, tệp luồng sẽ được định tuyến đến thất bại hoặc thử lại, và thuộc tính này "
        + "sẽ được điền đầy đủ với nguyên nhân của lỗi.")
public class PutDatabaseRecord extends AbstractProcessor {

    public static final String UPDATE_TYPE = "UPDATE";
    public static final String INSERT_TYPE = "INSERT";
    public static final String DELETE_TYPE = "DELETE";
    public static final String UPSERT_TYPE = "UPSERT";
    public static final String INSERT_IGNORE_TYPE = "INSERT_IGNORE";
    public static final String SQL_TYPE = "SQL";   // Không phải là giá trị được phép trong thuộc tính Loại Câu lệnh, phải được đặt bởi thuộc tính
    public static final String USE_ATTR_TYPE = "Use statement.type Attribute";
    public static final String USE_RECORD_PATH = "Use Record Path";

    static final String STATEMENT_TYPE_ATTRIBUTE = "statement.type";

    static final String PUT_DATABASE_RECORD_ERROR = "putdatabaserecord.error";

    static final AllowableValue IGNORE_UNMATCHED_FIELD = new AllowableValue("Ignore Unmatched Fields", "Ignore Unmatched Fields",
            "Bất kỳ trường nào trong tài liệu không thể được ánh xạ tới cột trong cơ sở dữ liệu đều bị bỏ qua");
    static final AllowableValue FAIL_UNMATCHED_FIELD = new AllowableValue("Fail on Unmatched Fields", "Fail on Unmatched Fields",
            "Nếu tài liệu có bất kỳ trường nào không thể được ánh xạ tới cột trong cơ sở dữ liệu, FlowFile sẽ được định tuyến đến mối quan hệ thất bại");
    static final AllowableValue IGNORE_UNMATCHED_COLUMN = new AllowableValue("Ignore Unmatched Columns",
            "Ignore Unmatched Columns",
            "Bất kỳ cột nào trong cơ sở dữ liệu không có trường trong tài liệu sẽ được coi là không bắt buộc. Không có thông báo nào sẽ được ghi nhật ký");
    static final AllowableValue WARNING_UNMATCHED_COLUMN = new AllowableValue("Warn on Unmatched Columns",
            "Warn on Unmatched Columns",
            "Bất kỳ cột nào trong cơ sở dữ liệu không có trường trong tài liệu sẽ được coi là không bắt buộc. Một cảnh báo sẽ được ghi nhật ký");
    static final AllowableValue FAIL_UNMATCHED_COLUMN = new AllowableValue("Fail on Unmatched Columns",
            "Fail on Unmatched Columns",
            "Một luồng sẽ thất bại nếu có bất kỳ cột nào trong cơ sở dữ liệu không có trường trong tài liệu. Một lỗi sẽ được ghi nhật ký");

    // Mối quan hệ
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFile được tạo thành công từ kết quả truy vấn SQL.")
            .build();

    static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("Một FlowFile được định tuyến đến mối quan hệ này nếu cơ sở dữ liệu không thể được cập nhật nhưng thử lại hoạt động có thể thành công")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Một FlowFile được định tuyến đến mối quan hệ này nếu cơ sở dữ liệu không thể được cập nhật và thử lại hoạt động cũng sẽ thất bại, "
                    + "chẳng hạn như truy vấn không hợp lệ hoặc vi phạm ràng buộc tính toàn vẹn")
            .build();

    protected static Set<Relationship> relationships;

    // Thuộc tính
    static final PropertyDescriptor RECORD_READER_FACTORY = new Builder()
            .name("put-db-record-record-reader")
            .displayName("Bộ đọc bản ghi")
            .description("Chỉ định Dịch vụ Bộ điều khiển sẽ được sử dụng để phân tích dữ liệu đến và xác định lược đồ của dữ liệu.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor STATEMENT_TYPE = new Builder()
            .name("put-db-record-statement-type")
            .displayName("Loại câu lệnh")
            .description("Chỉ định loại câu lệnh SQL để tạo. "
                    + "Vui lòng tham khảo tài liệu cơ sở dữ liệu để tìm hiểu mô tả về hành vi của mỗi hoạt động. "
                    + "Vui lòng lưu ý rằng một số Loại Cơ sở dữ liệu có thể không hỗ trợ một số Loại Câu lệnh. "
                    + "Nếu 'Sử dụng thuộc tính statement.type' được chọn, thì giá trị được lấy từ thuộc tính statement.type trong "
                    + "FlowFile. Tùy chọn 'Sử dụng thuộc tính statement.type' là tùy chọn duy nhất cho phép loại câu lệnh 'SQL'. Nếu 'SQL' được chỉ định, giá trị của trường được chỉ định bởi "
                    + "thuộc tính 'Trường chứa SQL' dự kiến sẽ là một câu lệnh SQL hợp lệ trên cơ sở dữ liệu đích, và sẽ được thực thi nguyên trạng.")
            .required(true)
            .allowableValues(UPDATE_TYPE, INSERT_TYPE, UPSERT_TYPE, INSERT_IGNORE_TYPE, DELETE_TYPE, USE_ATTR_TYPE, USE_RECORD_PATH)
            .build();

    static final PropertyDescriptor STATEMENT_TYPE_RECORD_PATH = new Builder()
        .name("Đường dẫn bản ghi loại câu lệnh")
        .displayName("Đường dẫn bản ghi loại câu lệnh")
        .description("Chỉ định một RecordPath để đánh giá so với mỗi Bản ghi để xác định Loại Câu lệnh. RecordPath phải bằng INSERT, UPDATE, UPSERT hoặc DELETE.")
        .required(true)
        .addValidator(new RecordPathValidator())
        .expressionLanguageSupported(NONE)
        .dependsOn(STATEMENT_TYPE, USE_RECORD_PATH)
        .build();

    static final PropertyDescriptor DATA_RECORD_PATH = new Builder()
        .name("Đường dẫn bản ghi dữ liệu")
        .displayName("Đường dẫn bản ghi dữ liệu")
        .description("Nếu được chỉ định, thuộc tính này biểu thị một RecordPath sẽ được đánh giá so với mỗi Bản ghi đến và Bản ghi kết quả từ việc đánh giá RecordPath sẽ được gửi đến"
            + " cơ sở dữ liệu thay vì gửi toàn bộ Bản ghi đến. Nếu không được chỉ định, toàn bộ Bản ghi đến sẽ được xuất bản đến cơ sở dữ liệu.")
        .required(false)
        .addValidator(new RecordPathValidator())
        .expressionLanguageSupported(NONE)
        .build();

    static final PropertyDescriptor DBCP_SERVICE = new Builder()
            .name("put-db-record-dcbp-service")
            .displayName("Dịch vụ Hợp nhất Kết nối Cơ sở dữ liệu")
            .description("Dịch vụ Bộ điều khiển được sử dụng để lấy kết nối đến cơ sở dữ liệu để gửi bản ghi.")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    static final PropertyDescriptor CATALOG_NAME = new Builder()
            .name("put-db-record-catalog-name")
            .displayName("Tên Danh mục")
            .description("Tên danh mục mà câu lệnh phải cập nhật. Điều này có thể không áp dụng cho cơ sở dữ liệu mà bạn đang cập nhật. Trong trường hợp này, hãy để trường trống. Lưu ý rằng nếu "
                    + "thuộc tính được đặt và cơ sở dữ liệu phân biệt chữ hoa chữ thường, tên danh mục phải khớp chính xác với tên danh mục của cơ sở dữ liệu.")
            .required(false)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor SCHEMA_NAME = new Builder()
            .name("put-db-record-schema-name")
            .displayName("Tên Lược đồ")
            .description("Tên lược đồ mà bảng thuộc về. Điều này có thể không áp dụng cho cơ sở dữ liệu mà bạn đang cập nhật. Trong trường hợp này, hãy để trường trống. Lưu ý rằng nếu "
                    + "thuộc tính được đặt và cơ sở dữ liệu phân biệt chữ hoa chữ thường, tên lược đồ phải khớp chính xác với tên lược đồ của cơ sở dữ liệu.")
            .required(false)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor TABLE_NAME = new Builder()
            .name("put-db-record-table-name")
            .displayName("Tên Bảng")
            .description("Tên của bảng mà câu lệnh phải ảnh hưởng. Lưu ý rằng nếu cơ sở dữ liệu phân biệt chữ hoa chữ thường, tên bảng phải khớp chính xác với tên bảng của cơ sở dữ liệu.")
            .required(true)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final AllowableValue BINARY_STRING_FORMAT_UTF8 = new AllowableValue(
            "UTF-8",
            "UTF-8",
            "Giá trị chuỗi cho các cột nhị phân chứa giá trị ban đầu dưới dạng văn bản thông qua mã hóa ký tự UTF-8"
    );

    static final AllowableValue BINARY_STRING_FORMAT_HEX_STRING = new AllowableValue(
            "Hexadecimal",
            "Hexadecimal",
            "Giá trị chuỗi cho các cột nhị phân chứa giá trị ban đầu ở định dạng thập lục phân"
    );

    static final AllowableValue BINARY_STRING_FORMAT_BASE64 = new AllowableValue(
            "Base64",
            "Base64",
            "Giá trị chuỗi cho các cột nhị phân chứa giá trị ban đầu được mã hóa ở định dạng Base64"
    );

    static final PropertyDescriptor BINARY_STRING_FORMAT = new Builder()
            .name("put-db-record-binary-format")
            .displayName("Định dạng Chuỗi Nhị phân")
            .description("Định dạng sẽ được áp dụng khi giải mã các giá trị chuỗi thành nhị phân.")
            .required(false)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .allowableValues(BINARY_STRING_FORMAT_UTF8, BINARY_STRING_FORMAT_HEX_STRING, BINARY_STRING_FORMAT_BASE64)
            .defaultValue(BINARY_STRING_FORMAT_UTF8.getValue())
            .build();

    static final PropertyDescriptor TRANSLATE_FIELD_NAMES = new Builder()
            .name("put-db-record-translate-field-names")
            .displayName("Dịch Tên Trường")
            .description("Nếu đúng, Bộ xử lý sẽ cố gắng dịch tên trường thành tên cột thích hợp cho bảng được chỉ định. "
                    + "Nếu sai, tên trường phải khớp chính xác với tên cột, hoặc cột sẽ không được cập nhật")
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    static final PropertyDescriptor UNMATCHED_FIELD_BEHAVIOR = new Builder()
            .name("put-db-record-unmatched-field-behavior")
            .displayName("Hành vi Trường Không khớp")
            .description("Nếu bản ghi đến có trường không ánh xạ tới bất kỳ cột nào của bảng cơ sở dữ liệu, thuộc tính này chỉ định cách xử lý tình huống")
            .allowableValues(IGNORE_UNMATCHED_FIELD, FAIL_UNMATCHED_FIELD)
            .defaultValue(IGNORE_UNMATCHED_FIELD.getValue())
            .build();

    static final PropertyDescriptor UNMATCHED_COLUMN_BEHAVIOR = new Builder()
            .name("put-db-record-unmatched-column-behavior")
            .displayName("Hành vi Cột Không khớp")
            .description("Nếu bản ghi đến không có ánh xạ trường cho tất cả các cột của bảng cơ sở dữ liệu, thuộc tính này chỉ định cách xử lý tình huống")
            .allowableValues(IGNORE_UNMATCHED_COLUMN, WARNING_UNMATCHED_COLUMN, FAIL_UNMATCHED_COLUMN)
            .defaultValue(FAIL_UNMATCHED_COLUMN.getValue())
            .build();

    static final PropertyDescriptor UPDATE_KEYS = new Builder()
            .name("put-db-record-update-keys")
            .displayName("Khóa Cập nhật")
            .description("Danh sách tên cột được phân tách bằng dấu phẩy mà một cách duy nhất xác định một hàng trong cơ sở dữ liệu cho các câu lệnh UPDATE. "
                    + "Nếu Loại Câu lệnh là UPDATE và thuộc tính này không được đặt, các Khóa Chính của bảng sẽ được sử dụng. "
                    + "Trong trường hợp này, nếu không có Khóa Chính nào tồn tại, việc chuyển đổi sang SQL sẽ thất bại nếu Hành vi Cột Không khớp được đặt thành FAIL. "
                    + "Thuộc tính này bị bỏ qua nếu Loại Câu lệnh là INSERT")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .dependsOn(STATEMENT_TYPE, UPDATE_TYPE, UPSERT_TYPE, SQL_TYPE, USE_ATTR_TYPE, USE_RECORD_PATH)
            .build();

    static final PropertyDescriptor FIELD_CONTAINING_SQL = new Builder()
            .name("put-db-record-field-containing-sql")
            .displayName("Trường Chứa SQL")
            .description("Nếu Loại Câu lệnh là 'SQL' (như được đặt trong thuộc tính statement.type), trường này cho biết trường nào trong bản ghi (các) chứa câu lệnh SQL sẽ được thực thi. Giá trị "
                    + "của trường phải là một câu lệnh SQL duy nhất. Nếu Loại Câu lệnh không phải 'SQL', trường này sẽ bị bỏ qua.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .dependsOn(STATEMENT_TYPE, USE_ATTR_TYPE, USE_RECORD_PATH)
            .build();

    static final PropertyDescriptor ALLOW_MULTIPLE_STATEMENTS = new Builder()
            .name("put-db-record-allow-multiple-statements")
            .displayName("Cho phép Nhiều Câu lệnh SQL")
            .description("Nếu Loại Câu lệnh là 'SQL' (như được đặt trong thuộc tính statement.type), trường này cho biết có phân tách giá trị trường theo dấu chấm phẩy và thực thi từng câu lệnh "
                    + "riêng biệt hay không. Nếu bất kỳ câu lệnh nào gây ra lỗi, toàn bộ bộ câu lệnh sẽ được khôi phục lại. Nếu Loại Câu lệnh không phải 'SQL', trường này sẽ bị bỏ qua.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .dependsOn(STATEMENT_TYPE, USE_ATTR_TYPE, USE_RECORD_PATH)
            .build();

    static final PropertyDescriptor QUOTE_IDENTIFIERS = new Builder()
            .name("put-db-record-quoted-identifiers")
            .displayName("Đặt trong ngoặc Định danh Cột")
            .description("Bật tùy chọn này sẽ khiến tất cả tên cột được đặt trong ngoặc, cho phép bạn sử dụng các từ dành riêng làm tên cột trong bảng của bạn.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    static final PropertyDescriptor QUOTE_TABLE_IDENTIFIER = new Builder()
            .name("put-db-record-quoted-table-identifiers")
            .displayName("Đặt trong ngoặc Định danh Bảng")
            .description("Bật tùy chọn này sẽ khiến tên bảng được đặt trong ngoặc để hỗ trợ sử dụng các ký tự đặc biệt trong tên bảng.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    static final PropertyDescriptor QUERY_TIMEOUT = new Builder()
            .name("put-db-record-query-timeout")
            .displayName("Thời gian Chờ Tối đa")
            .description("Khoảng thời gian tối đa được phép cho một câu lệnh SQL đang chạy "
                    + ", không có giới hạn có nghĩa là không có giới hạn. Thời gian tối đa nhỏ hơn 1 giây sẽ bằng không.")
            .defaultValue("0 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor TABLE_SCHEMA_CACHE_SIZE = new Builder()
            .name("table-schema-cache-size")
            .displayName("Kích thước Bộ nhớ đệm Lược đồ Bảng")
            .description("Chỉ định có bao nhiêu Lược đồ Bảng sẽ được lưu vào bộ nhớ đệm")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("100")
            .required(true)
            .build();

    static final PropertyDescriptor MAX_BATCH_SIZE = new Builder()
            .name("put-db-record-max-batch-size")
            .displayName("Kích thước Lô Tối đa")
            .description("Chỉ định số lượng tối đa các câu lệnh sql sẽ được đưa vào mỗi lô được gửi đến cơ sở dữ liệu. Không có giới hạn có nghĩa là kích thước lô không bị giới hạn, "
                    + "và tất cả các câu lệnh được đưa vào một lô duy nhất có thể gây ra vấn đề sử dụng bộ nhớ cao đối với một số lượng rất lớn các câu lệnh.")
            .defaultValue("1000")
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor AUTO_COMMIT = new PropertyDescriptor.Builder()
            .name("database-session-autocommit")
            .displayName("Tự động Xác nhận Phiên Cơ sở dữ liệu")
            .description("Chế độ tự động xác nhận sẽ được đặt trên kết nối cơ sở dữ liệu được sử dụng. Nếu được đặt thành sai, hoạt động (các) sẽ được xác nhận hoặc khôi phục rõ ràng "
                    + "(dựa trên thành công hoặc thất bại tương ứng). Nếu được đặt thành đúng, trình điều khiển/cơ sở dữ liệu sẽ tự động xử lý việc xác nhận/khôi phục. "
                    + "Đặt thuộc tính này thành 'Không có giá trị' sẽ để nguyên chế độ tự động xác nhận của kết nối cơ sở dữ liệu.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(false)
            .build();
    static final PropertyDescriptor DB_TYPE;

    protected static final Map<String, DatabaseAdapter> dbAdapters;
    protected static List<PropertyDescriptor> propDescriptors;
    private Cache<SchemaKey, TableSchema> schemaCache;

    static {
        dbAdapters = new HashMap<>();
        ArrayList<AllowableValue> dbAdapterValues = new ArrayList<>();

        ServiceLoader<DatabaseAdapter> dbAdapterLoader = ServiceLoader.load(DatabaseAdapter.class);
        dbAdapterLoader.forEach(databaseAdapter -> {
            dbAdapters.put(databaseAdapter.getName(), databaseAdapter);
            dbAdapterValues.add(new AllowableValue(databaseAdapter.getName(), databaseAdapter.getName(), databaseAdapter.getDescription()));
        });

        DB_TYPE = new Builder()
            .name("db-type")
            .displayName("Database Type")
            .description("The type/flavor of database, used for generating database-specific code. In many cases the Generic type "
                + "should suffice, but some databases (such as Oracle) require custom SQL clauses. ")
            .allowableValues(dbAdapterValues.toArray(new AllowableValue[0]))
            .defaultValue("Generic")
            .required(false)
            .build();

        final Set<Relationship> r = new HashSet<>();
        r.add(REL_SUCCESS);
        r.add(REL_FAILURE);
        r.add(REL_RETRY);
        relationships = Collections.unmodifiableSet(r);

        final List<PropertyDescriptor> pds = new ArrayList<>();
        pds.add(RECORD_READER_FACTORY);
        pds.add(DB_TYPE);
        pds.add(STATEMENT_TYPE);
        pds.add(STATEMENT_TYPE_RECORD_PATH);
        pds.add(DATA_RECORD_PATH);
        pds.add(DBCP_SERVICE);
        pds.add(CATALOG_NAME);
        pds.add(SCHEMA_NAME);
        pds.add(TABLE_NAME);
        pds.add(BINARY_STRING_FORMAT);
        pds.add(TRANSLATE_FIELD_NAMES);
        pds.add(UNMATCHED_FIELD_BEHAVIOR);
        pds.add(UNMATCHED_COLUMN_BEHAVIOR);
        pds.add(UPDATE_KEYS);
        pds.add(FIELD_CONTAINING_SQL);
        pds.add(ALLOW_MULTIPLE_STATEMENTS);
        pds.add(QUOTE_IDENTIFIERS);
        pds.add(QUOTE_TABLE_IDENTIFIER);
        pds.add(QUERY_TIMEOUT);
        pds.add(RollbackOnFailure.ROLLBACK_ON_FAILURE);
        pds.add(TABLE_SCHEMA_CACHE_SIZE);
        pds.add(MAX_BATCH_SIZE);
        pds.add(AUTO_COMMIT);

        propDescriptors = Collections.unmodifiableList(pds);
    }

    private DatabaseAdapter databaseAdapter;
    private volatile Function<Record, String> recordPathOperationType;
    private volatile RecordPath dataRecordPath;


    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propDescriptors;
    }

    // TODO remove this at next major release as dynamic properties are not used by this processor
    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
                .dynamic(true)
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Collection<ValidationResult> validationResults = new ArrayList<>(super.customValidate(validationContext));

        DatabaseAdapter databaseAdapter = dbAdapters.get(validationContext.getProperty(DB_TYPE).getValue());
        String statementType = validationContext.getProperty(STATEMENT_TYPE).getValue();
        if ((UPSERT_TYPE.equals(statementType) && !databaseAdapter.supportsUpsert())
            || (INSERT_IGNORE_TYPE.equals(statementType) && !databaseAdapter.supportsInsertIgnore())) {
            validationResults.add(new ValidationResult.Builder()
                .subject(STATEMENT_TYPE.getDisplayName())
                .valid(false)
                .explanation(databaseAdapter.getName() + " does not support " + statementType)
                .build()
            );
        }

        final Boolean autoCommit = validationContext.getProperty(AUTO_COMMIT).asBoolean();
        final boolean rollbackOnFailure = validationContext.getProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE).asBoolean();
        if (autoCommit != null && autoCommit && rollbackOnFailure) {
            validationResults.add(new ValidationResult.Builder()
                    .subject(RollbackOnFailure.ROLLBACK_ON_FAILURE.getDisplayName())
                    .explanation(format("'%s' cannot be set to 'true' when '%s' is also set to 'true'. "
                                    + "Transaction rollbacks for batch updates cannot rollback all the flow file's statements together "
                                    + "when auto commit is set to 'true' because the database autocommits each batch separately.",
                            RollbackOnFailure.ROLLBACK_ON_FAILURE.getDisplayName(), AUTO_COMMIT.getDisplayName()))
                    .build());
        }

        if (autoCommit != null && autoCommit && !isMaxBatchSizeHardcodedToZero(validationContext)) {
                final String explanation = format("'%s' must be hard-coded to zero when '%s' is set to 'true'."
                                + " Batch size equal to zero executes all statements in a single transaction"
                                + " which allows rollback to revert all the flow file's statements together if an error occurs.",
                        MAX_BATCH_SIZE.getDisplayName(), AUTO_COMMIT.getDisplayName());

                validationResults.add(new ValidationResult.Builder()
                        .subject(MAX_BATCH_SIZE.getDisplayName())
                        .explanation(explanation)
                        .build());
        }

        return validationResults;
    }

    private boolean isMaxBatchSizeHardcodedToZero(ValidationContext validationContext) {
        try {
            return !validationContext.getProperty(MAX_BATCH_SIZE).isExpressionLanguagePresent()
                    && 0 == validationContext.getProperty(MAX_BATCH_SIZE).asInteger();
        } catch (Exception ex) {
            return false;
        }
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        databaseAdapter = dbAdapters.get(context.getProperty(DB_TYPE).getValue());

        final int tableSchemaCacheSize = context.getProperty(TABLE_SCHEMA_CACHE_SIZE).asInteger();
        schemaCache = Caffeine.newBuilder()
                .maximumSize(tableSchemaCacheSize)
                .build();

        final String statementTypeRecordPathValue = context.getProperty(STATEMENT_TYPE_RECORD_PATH).getValue();
        if (statementTypeRecordPathValue == null) {
            recordPathOperationType = null;
        } else {
            final RecordPath recordPath = RecordPath.compile(statementTypeRecordPathValue);
            recordPathOperationType = new RecordPathStatementType(recordPath);
        }

        final String dataRecordPathValue = context.getProperty(DATA_RECORD_PATH).getValue();
        dataRecordPath = dataRecordPathValue == null ? null : RecordPath.compile(dataRecordPathValue);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);

        Connection connection = null;
        boolean originalAutoCommit = false;
        try {
            connection = dbcpService.getConnection(flowFile.getAttributes());

            originalAutoCommit = connection.getAutoCommit();
            final Boolean propertyAutoCommitValue = context.getProperty(AUTO_COMMIT).asBoolean();
            if (propertyAutoCommitValue != null && originalAutoCommit != propertyAutoCommitValue) {
                try {
                    connection.setAutoCommit(propertyAutoCommitValue);
                } catch (Exception ex) {
                    getLogger().debug("Failed to setAutoCommit({}) due to {}", propertyAutoCommitValue, ex.getClass().getName(), ex);
                }
            }

            putToDatabase(context, session, flowFile, connection);

            // If the connection's auto-commit setting is false, then manually commit the transaction
            if (!connection.getAutoCommit()) {
                connection.commit();
            }

            session.transfer(flowFile, REL_SUCCESS);
            session.getProvenanceReporter().send(flowFile, getJdbcUrl(connection));
        } catch (final Exception e) {
            routeOnException(context, session, connection, e, flowFile);
        } finally {
            closeConnection(connection, originalAutoCommit);
        }
    }

    private void routeOnException(final ProcessContext context, final ProcessSession session,
                                  Connection connection, Exception e, FlowFile flowFile) {
        // When an Exception is thrown, we want to route to 'retry' if we expect that attempting the same request again
        // might work. Otherwise, route to failure. SQLTransientException is a specific type that indicates that a retry may work.
        final Relationship relationship;
        final Throwable toAnalyze = (e instanceof BatchUpdateException || e instanceof ProcessException) ? e.getCause() : e;
        if (toAnalyze instanceof SQLTransientException) {
            relationship = REL_RETRY;
            flowFile = session.penalize(flowFile);
        } else {
            relationship = REL_FAILURE;
        }

        final boolean rollbackOnFailure = context.getProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE).asBoolean();
        if (rollbackOnFailure) {
            getLogger().error("Failed to put Records to database for {}. Rolling back NiFi session and returning the flow file to its incoming queue.", flowFile, e);
            session.rollback();
            context.yield();
        } else {
            getLogger().error("Failed to put Records to database for {}. Routing to {}.", flowFile, relationship, e);
            flowFile = session.putAttribute(flowFile, PUT_DATABASE_RECORD_ERROR, (e.getMessage() == null ? "Unknown": e.getMessage()));
            session.transfer(flowFile, relationship);
        }

        rollbackConnection(connection);
    }

    private void rollbackConnection(Connection connection) {
        if (connection != null) {
            try {
                if (!connection.getAutoCommit()) {
                    connection.rollback();
                    getLogger().debug("Manually rolled back JDBC transaction.");
                }
            } catch (final Exception rollbackException) {
                getLogger().error("Failed to rollback JDBC transaction", rollbackException);
            }
        }
    }

    private void closeConnection(Connection connection, boolean originalAutoCommit) {
        if (connection != null) {
            try {
                if (originalAutoCommit != connection.getAutoCommit()) {
                    connection.setAutoCommit(originalAutoCommit);
                }
            } catch (final Exception autoCommitException) {
                getLogger().warn(String.format("Failed to set auto-commit back to %s on connection", originalAutoCommit), autoCommitException);
            }

            try {
                if (!connection.isClosed()) {
                    connection.close();
                }
            } catch (final Exception closeException) {
                getLogger().warn("Failed to close database connection", closeException);
            }
        }
    }

    private void executeSQL(final ProcessContext context, final ProcessSession session, final FlowFile flowFile,
                            final Connection connection, final RecordReader recordReader)
            throws IllegalArgumentException, MalformedRecordException, IOException, SQLException {

        final RecordSchema recordSchema = recordReader.getSchema();

        // Find which field has the SQL statement in it
        final String sqlField = context.getProperty(FIELD_CONTAINING_SQL).evaluateAttributeExpressions(flowFile).getValue();
        if (StringUtils.isEmpty(sqlField)) {
            throw new IllegalArgumentException(format("SQL specified as Statement Type but no Field Containing SQL was found, FlowFile %s", flowFile));
        }

        boolean schemaHasSqlField = recordSchema.getFields().stream().anyMatch((field) -> sqlField.equals(field.getFieldName()));
        if (!schemaHasSqlField) {
            throw new IllegalArgumentException(format("Record schema does not contain Field Containing SQL: %s, FlowFile %s", sqlField, flowFile));
        }

        try (final Statement statement = connection.createStatement()) {
            final int timeoutMillis = context.getProperty(QUERY_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS).intValue();
            try {
                statement.setQueryTimeout(timeoutMillis); // timeout in seconds
            } catch (SQLException se) {
                // If the driver doesn't support query timeout, then assume it is "infinite". Allow a timeout of zero only
                if (timeoutMillis > 0) {
                    throw se;
                }
            }

            final ComponentLog log = getLogger();
            final int maxBatchSize = context.getProperty(MAX_BATCH_SIZE).evaluateAttributeExpressions(flowFile).asInteger();
            // Batch Size 0 means put all sql statements into one batch update no matter how many statements there are.
            // Do not use batch statements if batch size is equal to 1 because that is the same as not using batching.
            // Also do not use batches if the connection does not support batching.
            boolean useBatch = maxBatchSize != 1 && isSupportsBatchUpdates(connection);
            int currentBatchSize = 0;
            int batchIndex = 0;

            boolean isFirstRecord = true;
            Record nextRecord = recordReader.nextRecord();
            while (nextRecord != null) {
                Record currentRecord = nextRecord;
                final String sql = currentRecord.getAsString(sqlField);
                nextRecord = recordReader.nextRecord();

                if (sql == null || StringUtils.isEmpty(sql)) {
                    throw new MalformedRecordException(format("Record had no (or null) value for Field Containing SQL: %s, FlowFile %s", sqlField, flowFile));
                }

                final String[] sqlStatements;
                if (context.getProperty(ALLOW_MULTIPLE_STATEMENTS).asBoolean()) {
                    final String regex = "(?<!\\\\);";
                    sqlStatements = (sql).split(regex);
                } else {
                    sqlStatements = new String[] { sql };
                }

                if (isFirstRecord) {
                    // If there is only one sql statement to process, then do not use batching.
                    if (nextRecord == null && sqlStatements.length == 1) {
                        useBatch = false;
                    }
                    isFirstRecord = false;
                }

                for (String sqlStatement : sqlStatements) {
                    if (useBatch) {
                        currentBatchSize++;
                        statement.addBatch(sqlStatement);
                    } else {
                        statement.execute(sqlStatement);
                    }
                }

                if (useBatch && maxBatchSize > 0 && currentBatchSize >= maxBatchSize) {
                    batchIndex++;
                    log.debug("Executing batch with last query {} because batch reached max size %s for {}; batch index: {}; batch size: {}",
                            sql, maxBatchSize, flowFile, batchIndex, currentBatchSize);
                    statement.executeBatch();
                    session.adjustCounter("Batches Executed", 1, false);
                    currentBatchSize = 0;
                }
            }

            if (useBatch && currentBatchSize > 0) {
                batchIndex++;
                log.debug("Executing last batch because last statement reached for {}; batch index: {}; batch size: {}",
                        flowFile, batchIndex, currentBatchSize);
                statement.executeBatch();
                session.adjustCounter("Batches Executed", 1, false);
            }
        }
    }


    private void executeDML(final ProcessContext context, final ProcessSession session, final FlowFile flowFile,
                            final Connection con, final RecordReader recordReader, final String explicitStatementType, final DMLSettings settings)
        throws IllegalArgumentException, MalformedRecordException, IOException, SQLException {

        final ComponentLog log = getLogger();

        final String catalog = context.getProperty(CATALOG_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String schemaName = context.getProperty(SCHEMA_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String updateKeys = context.getProperty(UPDATE_KEYS).evaluateAttributeExpressions(flowFile).getValue();
        final int maxBatchSize = context.getProperty(MAX_BATCH_SIZE).evaluateAttributeExpressions(flowFile).asInteger();
        final int timeoutMillis = context.getProperty(QUERY_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS).intValue();

        final String binaryStringFormat = context.getProperty(BINARY_STRING_FORMAT).evaluateAttributeExpressions(flowFile).getValue();

        // Ensure the table name has been set, the generated SQL statements (and TableSchema cache) will need it
        if (StringUtils.isEmpty(tableName)) {
            throw new IllegalArgumentException(format("Cannot process %s because Table Name is null or empty", flowFile));
        }

        final SchemaKey schemaKey = new PutDatabaseRecord.SchemaKey(catalog, schemaName, tableName);
        final TableSchema tableSchema;
        try {
            tableSchema = schemaCache.get(schemaKey, key -> {
                try {
                    final TableSchema schema = TableSchema.from(con, catalog, schemaName, tableName, settings.translateFieldNames, updateKeys, log);
                    getLogger().debug("Fetched Table Schema {} for table name {}", schema, tableName);
                    return schema;
                } catch (SQLException e) {
                    // Wrap this in a runtime exception, it is unwrapped in the outer try
                    throw new ProcessException(e);
                }
            });
            if (tableSchema == null) {
                throw new IllegalArgumentException("No table schema specified!");
            }
        } catch (ProcessException pe) {
            // Unwrap the SQLException if one occurred
            if (pe.getCause() instanceof SQLException) {
                throw (SQLException) pe.getCause();
            } else {
                throw pe;
            }
        }

        // build the fully qualified table name
        final String fqTableName =  generateTableName(settings, catalog, schemaName, tableName, tableSchema);

        final Map<String, PreparedSqlAndColumns> preparedSql = new HashMap<>();
        int currentBatchSize = 0;
        int batchIndex = 0;
        Record outerRecord;
        PreparedStatement lastPreparedStatement = null;

        try {
            while ((outerRecord = recordReader.nextRecord()) != null) {
                final String statementType;
                if (USE_RECORD_PATH.equalsIgnoreCase(explicitStatementType)) {
                    statementType = recordPathOperationType.apply(outerRecord);
                } else {
                    statementType = explicitStatementType;
                }

                final List<Record> dataRecords = getDataRecords(outerRecord);
                for (final Record currentRecord : dataRecords) {
                    PreparedSqlAndColumns preparedSqlAndColumns = preparedSql.get(statementType);
                    if (preparedSqlAndColumns == null) {
                        final RecordSchema recordSchema = currentRecord.getSchema();

                        final SqlAndIncludedColumns sqlHolder;
                        if (INSERT_TYPE.equalsIgnoreCase(statementType)) {
                            sqlHolder = generateInsert(recordSchema, fqTableName, tableSchema, settings);
                        } else if (UPDATE_TYPE.equalsIgnoreCase(statementType)) {
                            sqlHolder = generateUpdate(recordSchema, fqTableName, updateKeys, tableSchema, settings);
                        } else if (DELETE_TYPE.equalsIgnoreCase(statementType)) {
                            sqlHolder = generateDelete(recordSchema, fqTableName, tableSchema, settings);
                        } else if (UPSERT_TYPE.equalsIgnoreCase(statementType)) {
                            sqlHolder = generateUpsert(recordSchema, fqTableName, updateKeys, tableSchema, settings);
                        } else if (INSERT_IGNORE_TYPE.equalsIgnoreCase(statementType)) {
                            sqlHolder = generateInsertIgnore(recordSchema, fqTableName, updateKeys, tableSchema, settings);
                        } else {
                            throw new IllegalArgumentException(format("Statement Type %s is not valid, FlowFile %s", statementType, flowFile));
                        }

                        // Log debug sqlHolder
                        log.debug("Generated SQL: {}", sqlHolder.getSql());
                        // Create the Prepared Statement
                        final PreparedStatement preparedStatement = con.prepareStatement(sqlHolder.getSql());

                        try {
                            preparedStatement.setQueryTimeout(timeoutMillis); // timeout in seconds
                        } catch (final SQLException se) {
                            // If the driver doesn't support query timeout, then assume it is "infinite". Allow a timeout of zero only
                            if (timeoutMillis > 0) {
                                throw se;
                            }
                        }

                        preparedSqlAndColumns = new PreparedSqlAndColumns(sqlHolder, preparedStatement);
                        preparedSql.put(statementType, preparedSqlAndColumns);
                    }

                    final PreparedStatement ps = preparedSqlAndColumns.getPreparedStatement();
                    final List<Integer> fieldIndexes = preparedSqlAndColumns.getSqlAndIncludedColumns().getFieldIndexes();
                    final String sql = preparedSqlAndColumns.getSqlAndIncludedColumns().getSql();

                    if (ps != lastPreparedStatement && lastPreparedStatement != null) {
                        batchIndex++;
                        log.debug("Executing query {} because Statement Type changed between Records for {}; fieldIndexes: {}; batch index: {}; batch size: {}",
                            sql, flowFile, fieldIndexes, batchIndex, currentBatchSize);
                        lastPreparedStatement.executeBatch();

                        session.adjustCounter("Batches Executed", 1, false);
                        currentBatchSize = 0;
                    }
                    lastPreparedStatement = ps;

                    final Object[] values = currentRecord.getValues();
                    final List<DataType> dataTypes = currentRecord.getSchema().getDataTypes();
                    final RecordSchema recordSchema = currentRecord.getSchema();
                    final Map<String, ColumnDescription> columns = tableSchema.getColumns();

                    int deleteIndex = 0;
                    for (int i = 0; i < fieldIndexes.size(); i++) {
                        final int currentFieldIndex = fieldIndexes.get(i);
                        Object currentValue = values[currentFieldIndex];
                        final DataType dataType = dataTypes.get(currentFieldIndex);
                        final int fieldSqlType = DataTypeUtils.getSQLTypeValue(dataType);
                        final String fieldName = recordSchema.getField(currentFieldIndex).getFieldName();
                        String columnName = ColumnDescription.normalizeColumnName(fieldName, settings.translateFieldNames);
                        int sqlType;

                        final ColumnDescription column = columns.get(columnName);
                        // 'column' should not be null here as the fieldIndexes should correspond to fields that match table columns, but better to handle just in case
                        if (column == null) {
                            if (!settings.ignoreUnmappedFields) {
                                throw new SQLDataException("Cannot map field '" + fieldName + "' to any column in the database\n"
                                        + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", columns.keySet()));
                            } else {
                                sqlType = fieldSqlType;
                            }
                        } else {
                            sqlType = column.getDataType();
                            // SQLServer returns -150 for sql_variant from DatabaseMetaData though the server expects -156 when setting a sql_variant parameter
                            if (sqlType == -150) {
                                sqlType = -156;
                            }
                        }

                        // Convert (if necessary) from field data type to column data type
                        if (fieldSqlType != sqlType) {
                            try {
                                DataType targetDataType = DataTypeUtils.getDataTypeFromSQLTypeValue(sqlType);
                                // If sqlType is unsupported, fall back to the fieldSqlType instead
                                if (targetDataType == null) {
                                    targetDataType = DataTypeUtils.getDataTypeFromSQLTypeValue(fieldSqlType);
                                }
                                if (targetDataType != null) {
                                    if (sqlType == Types.BLOB || sqlType == Types.BINARY || sqlType == Types.VARBINARY || sqlType == Types.LONGVARBINARY) {
                                        if (currentValue instanceof Object[]) {
                                            // Convert Object[Byte] arrays to byte[]
                                            Object[] src = (Object[]) currentValue;
                                            if (src.length > 0) {
                                                if (!(src[0] instanceof Byte)) {
                                                    throw new IllegalTypeConversionException("Cannot convert value " + currentValue + " to BLOB/BINARY/VARBINARY/LONGVARBINARY");
                                                }
                                            }
                                            byte[] dest = new byte[src.length];
                                            for (int j = 0; j < src.length; j++) {
                                                dest[j] = (Byte) src[j];
                                            }
                                            currentValue = dest;
                                        } else if (currentValue instanceof String) {
                                            final String stringValue = (String) currentValue;

                                            if (BINARY_STRING_FORMAT_BASE64.getValue().equals(binaryStringFormat)) {
                                                currentValue = BaseEncoding.base64().decode(stringValue);
                                            } else if (BINARY_STRING_FORMAT_HEX_STRING.getValue().equals(binaryStringFormat)) {
                                                currentValue = BaseEncoding.base16().decode(stringValue.toUpperCase());
                                            } else {
                                                currentValue = stringValue.getBytes(StandardCharsets.UTF_8);
                                            }
                                        } else if (currentValue != null && !(currentValue instanceof byte[])) {
                                            throw new IllegalTypeConversionException("Cannot convert value " + currentValue + " to BLOB/BINARY/VARBINARY/LONGVARBINARY");
                                        }
                                    } else {
                                        currentValue = DataTypeUtils.convertType(
                                                currentValue,
                                                targetDataType,
                                                fieldName);
                                    }
                                }
                            } catch (IllegalTypeConversionException itce) {
                                // If the field and column types don't match or the value can't otherwise be converted to the column datatype,
                                // try with the original object and field datatype
                                sqlType = DataTypeUtils.getSQLTypeValue(dataType);
                            }
                        }

                        // If DELETE type, insert the object twice if the column is nullable because of the null check (see generateDelete for details)
                        if (DELETE_TYPE.equalsIgnoreCase(statementType)) {
                            setParameter(ps, ++deleteIndex, currentValue, fieldSqlType, sqlType);
                            if (column != null && column.isNullable()) {
                                setParameter(ps, ++deleteIndex, currentValue, fieldSqlType, sqlType);
                            }
                        } else if (UPSERT_TYPE.equalsIgnoreCase(statementType)) {
                            final int timesToAddObjects = databaseAdapter.getTimesToAddColumnObjectsForUpsert();
                            for (int j = 0; j < timesToAddObjects; j++) {
                                setParameter(ps, i + (fieldIndexes.size() * j) + 1, currentValue, fieldSqlType, sqlType);
                            }
                        } else {
                            setParameter(ps, i + 1, currentValue, fieldSqlType, sqlType);
                        }
                    }

                    ps.addBatch();
                    session.adjustCounter(statementType + " updates performed", 1, false);
                    if (++currentBatchSize == maxBatchSize) {
                        batchIndex++;
                        log.debug("Executing query {} because batch reached max size for {}; fieldIndexes: {}; batch index: {}; batch size: {}",
                            sql, flowFile, fieldIndexes, batchIndex, currentBatchSize);
                        session.adjustCounter("Batches Executed", 1, false);
                        ps.executeBatch();
                        currentBatchSize = 0;
                    }
                }
            }

            if (currentBatchSize > 0) {
                lastPreparedStatement.executeBatch();
                session.adjustCounter("Batches Executed", 1, false);
            }
        } finally {
            for (final PreparedSqlAndColumns preparedSqlAndColumns : preparedSql.values()) {
                preparedSqlAndColumns.getPreparedStatement().close();
            }
        }
    }

    private void setParameter(PreparedStatement ps, int index, Object value, int fieldSqlType, int sqlType) throws IOException {
        if (sqlType == Types.BLOB) {
            // Convert Byte[] or String (anything that has been converted to byte[]) into BLOB
            if (fieldSqlType == Types.ARRAY || fieldSqlType == Types.VARCHAR) {
                if (!(value instanceof byte[])) {
                    if (value == null) {
                        try {
                            ps.setNull(index, Types.BLOB);
                            return;
                        } catch (SQLException e) {
                            throw new IOException("Unable to setNull() on prepared statement" , e);
                        }
                    } else {
                        throw new IOException("Expected BLOB to be of type byte[] but is instead " + value.getClass().getName());
                    }
                }
                byte[] byteArray = (byte[]) value;
                try (InputStream inputStream = new ByteArrayInputStream(byteArray)) {
                    ps.setBlob(index, inputStream);
                } catch (SQLException e) {
                    throw new IOException("Unable to parse binary data " + value, e);
                }
            } else {
                try (InputStream inputStream = new ByteArrayInputStream(value.toString().getBytes(StandardCharsets.UTF_8))) {
                    ps.setBlob(index, inputStream);
                } catch (IOException | SQLException e) {
                    throw new IOException("Unable to parse binary data " + value, e);
                }
            }
        } else if (sqlType == Types.CLOB) {
            if (value == null) {
                try {
                    ps.setNull(index, Types.CLOB);
                } catch (SQLException e) {
                    throw new IOException("Unable to setNull() on prepared statement", e);
                }
            } else {
                try {
                    Clob clob = ps.getConnection().createClob();
                    clob.setString(1, value.toString());
                    ps.setClob(index, clob);
                } catch (SQLException e) {
                    throw new IOException("Unable to parse data as CLOB/String " + value, e);
                }
            }
        } else if (sqlType == Types.VARBINARY || sqlType == Types.LONGVARBINARY) {
            if (fieldSqlType == Types.ARRAY || fieldSqlType == Types.VARCHAR) {
                if (!(value instanceof byte[])) {
                    if (value == null) {
                        try {
                            ps.setNull(index, Types.BLOB);
                            return;
                        } catch (SQLException e) {
                            throw new IOException("Unable to setNull() on prepared statement" , e);
                        }
                    } else {
                        throw new IOException("Expected VARBINARY/LONGVARBINARY to be of type byte[] but is instead " + value.getClass().getName());
                    }
                }
                byte[] byteArray = (byte[]) value;
                try {
                    ps.setBytes(index, byteArray);
                } catch (SQLException e) {
                    throw new IOException("Unable to parse binary data with size" + byteArray.length, e);
                }
            } else {
                byte[] byteArray = new byte[0];
                try {
                    byteArray = value.toString().getBytes(StandardCharsets.UTF_8);
                    ps.setBytes(index, byteArray);
                } catch (SQLException e) {
                    throw new IOException("Unable to parse binary data with size" + byteArray.length, e);
                }
            }
        } else {
            try {
                // If the specified field type is OTHER and the SQL type is VARCHAR, the conversion went ok as a string literal but try the OTHER type when setting the parameter. If an error occurs,
                // try the normal way of using the sqlType
                // This helps with PostgreSQL enums and possibly other scenarios
                if (fieldSqlType == Types.OTHER && sqlType == Types.VARCHAR) {
                    try {
                        ps.setObject(index, value, fieldSqlType);
                    } catch (SQLException e) {
                        // Fall back to default setObject params
                        ps.setObject(index, value, sqlType);
                    }
                } else {
                    ps.setObject(index, value, sqlType);
                }
            } catch (SQLException e) {
                throw new IOException("Unable to setObject() with value " + value + " at index " + index + " of type " + sqlType , e);
            }
        }
    }

    private List<Record> getDataRecords(final Record outerRecord) {
        if (dataRecordPath == null) {
            return Collections.singletonList(outerRecord);
        }

        final RecordPathResult result = dataRecordPath.evaluate(outerRecord);
        final List<FieldValue> fieldValues = result.getSelectedFields().collect(Collectors.toList());
        if (fieldValues.isEmpty()) {
            throw new ProcessException("RecordPath " + dataRecordPath.getPath() + " evaluated against Record yielded no results.");
        }

        for (final FieldValue fieldValue : fieldValues) {
            final RecordFieldType fieldType = fieldValue.getField().getDataType().getFieldType();
            if (fieldType != RecordFieldType.RECORD) {
                throw new ProcessException("RecordPath " + dataRecordPath.getPath() + " evaluated against Record expected to return one or more Records but encountered field of type" +
                    " " + fieldType);
            }
        }

        final List<Record> dataRecords = new ArrayList<>(fieldValues.size());
        for (final FieldValue fieldValue : fieldValues) {
            dataRecords.add((Record) fieldValue.getValue());
        }

        return dataRecords;
    }

    private String getJdbcUrl(final Connection connection) {
        try {
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            if (databaseMetaData != null) {
                return databaseMetaData.getURL();
            }
        } catch (final Exception e) {
            getLogger().warn("Could not determine JDBC URL based on the Driver Connection.", e);
        }

        return "DBCPService";
    }

    private String getStatementType(final ProcessContext context, final FlowFile flowFile) {
        // Get the statement type from the attribute if necessary
        final String statementTypeProperty = context.getProperty(STATEMENT_TYPE).getValue();
        String statementType = statementTypeProperty;
        if (USE_ATTR_TYPE.equals(statementTypeProperty)) {
            statementType = flowFile.getAttribute(STATEMENT_TYPE_ATTRIBUTE);
        }

        return validateStatementType(statementType, flowFile);
    }

    private String validateStatementType(final String statementType, final FlowFile flowFile) {
        if (statementType == null || statementType.trim().isEmpty()) {
            throw new ProcessException("No Statement Type specified for " + flowFile);
        }

        if (INSERT_TYPE.equalsIgnoreCase(statementType) || UPDATE_TYPE.equalsIgnoreCase(statementType) || DELETE_TYPE.equalsIgnoreCase(statementType)
                || UPSERT_TYPE.equalsIgnoreCase(statementType) || SQL_TYPE.equalsIgnoreCase(statementType) || USE_RECORD_PATH.equalsIgnoreCase(statementType)
                || INSERT_IGNORE_TYPE.equalsIgnoreCase(statementType)) {

            return statementType;
        }

        throw new ProcessException("Invalid Statement Type <" + statementType + "> for " + flowFile);
    }

    private void putToDatabase(final ProcessContext context, final ProcessSession session, final FlowFile flowFile, final Connection connection) throws Exception {
        final String statementType = getStatementType(context, flowFile);

        try (final InputStream in = session.read(flowFile)) {
            final RecordReaderFactory recordReaderFactory = context.getProperty(RECORD_READER_FACTORY).asControllerService(RecordReaderFactory.class);
            final RecordReader recordReader = recordReaderFactory.createRecordReader(flowFile, in, getLogger());

            if (SQL_TYPE.equalsIgnoreCase(statementType)) {
                executeSQL(context, session, flowFile, connection, recordReader);
            } else {
                final DMLSettings settings = new DMLSettings(context);
                executeDML(context, session, flowFile, connection, recordReader, statementType, settings);
            }
        }
    }

    String generateTableName(final DMLSettings settings, final String catalog, final String schemaName, final String tableName, final TableSchema tableSchema) {
        final StringBuilder tableNameBuilder = new StringBuilder();
        if (catalog != null) {
            if (settings.quoteTableName) {
                tableNameBuilder.append(tableSchema.getQuotedIdentifierString())
                        .append(catalog)
                        .append(tableSchema.getQuotedIdentifierString());
            } else {
                tableNameBuilder.append(catalog);
            }

            tableNameBuilder.append(".");
        }

        if (schemaName != null) {
            if (settings.quoteTableName) {
                tableNameBuilder.append(tableSchema.getQuotedIdentifierString())
                        .append(schemaName)
                        .append(tableSchema.getQuotedIdentifierString());
            } else {
                tableNameBuilder.append(schemaName);
            }

            tableNameBuilder.append(".");
        }

        if (settings.quoteTableName) {
            tableNameBuilder.append(tableSchema.getQuotedIdentifierString())
                    .append(tableName)
                    .append(tableSchema.getQuotedIdentifierString());
        } else {
            tableNameBuilder.append(tableName);
        }

        return tableNameBuilder.toString();
    }

    private Set<String> getNormalizedColumnNames(final RecordSchema schema, final boolean translateFieldNames) {
        final Set<String> normalizedFieldNames = new HashSet<>();
        if (schema != null) {
            schema.getFieldNames().forEach((fieldName) -> normalizedFieldNames.add(ColumnDescription.normalizeColumnName(fieldName, translateFieldNames)));
        }
        return normalizedFieldNames;
    }

    SqlAndIncludedColumns generateInsert(final RecordSchema recordSchema, final String tableName, final TableSchema tableSchema, final DMLSettings settings)
            throws IllegalArgumentException, SQLException {

        checkValuesForRequiredColumns(recordSchema, tableSchema, settings);

        final StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("INSERT INTO ");
        sqlBuilder.append(tableName);
        sqlBuilder.append(" (");

        // iterate over all of the fields in the record, building the SQL statement by adding the column names
        List<String> fieldNames = recordSchema.getFieldNames();
        final List<Integer> includedColumns = new ArrayList<>();
        if (fieldNames != null) {
            int fieldCount = fieldNames.size();
            AtomicInteger fieldsFound = new AtomicInteger(0);

            for (int i = 0; i < fieldCount; i++) {
                RecordField field = recordSchema.getField(i);
                String fieldName = field.getFieldName();

                final ColumnDescription desc = tableSchema.getColumns().get(ColumnDescription.normalizeColumnName(fieldName, settings.translateFieldNames));
                if (desc == null && !settings.ignoreUnmappedFields) {
                    throw new SQLDataException("Cannot map field '" + fieldName + "' to any column in the database\n"
                            + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
                }

                if (desc != null) {
                    if (fieldsFound.getAndIncrement() > 0) {
                        sqlBuilder.append(", ");
                    }

                    if (settings.escapeColumnNames) {
                        sqlBuilder.append(tableSchema.getQuotedIdentifierString())
                                .append(desc.getColumnName())
                                .append(tableSchema.getQuotedIdentifierString());
                    } else {
                        sqlBuilder.append(desc.getColumnName());
                    }
                    includedColumns.add(i);
                } else {
                    // User is ignoring unmapped fields, but log at debug level just in case
                    getLogger().debug("Did not map field '" + fieldName + "' to any column in the database\n"
                            + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
                }
            }

            // complete the SQL statements by adding ?'s for all of the values to be escaped.
            sqlBuilder.append(") VALUES (");
            sqlBuilder.append(StringUtils.repeat("?", ",", includedColumns.size()));
            sqlBuilder.append(")");

            if (fieldsFound.get() == 0) {
                throw new SQLDataException("None of the fields in the record map to the columns defined by the " + tableName + " table\n"
                        + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
            }
        }
        return new SqlAndIncludedColumns(sqlBuilder.toString(), includedColumns);
    }

    SqlAndIncludedColumns generateUpsert(final RecordSchema recordSchema, final String tableName, final String updateKeys,
                                         final TableSchema tableSchema, final DMLSettings settings)
        throws IllegalArgumentException, SQLException, MalformedRecordException {

        checkValuesForRequiredColumns(recordSchema, tableSchema, settings);

        Set<String> keyColumnNames = getUpdateKeyColumnNames(tableName, updateKeys, tableSchema);
        normalizeKeyColumnNamesAndCheckForValues(recordSchema, updateKeys, settings, keyColumnNames);

        List<String> usedColumnNames = new ArrayList<>();
        List<Integer> usedColumnIndices = new ArrayList<>();

        List<String> fieldNames = recordSchema.getFieldNames();
        if (fieldNames != null) {
            int fieldCount = fieldNames.size();

            for (int i = 0; i < fieldCount; i++) {
                RecordField field = recordSchema.getField(i);
                String fieldName = field.getFieldName();

                final ColumnDescription desc = tableSchema.getColumns().get(ColumnDescription.normalizeColumnName(fieldName, settings.translateFieldNames));
                if (desc == null && !settings.ignoreUnmappedFields) {
                    throw new SQLDataException("Cannot map field '" + fieldName + "' to any column in the database\n"
                            + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
                }

                if (desc != null) {
                    if (settings.escapeColumnNames) {
                        usedColumnNames.add(tableSchema.getQuotedIdentifierString() + desc.getColumnName() + tableSchema.getQuotedIdentifierString());
                    } else {
                        usedColumnNames.add(desc.getColumnName());
                    }
                    usedColumnIndices.add(i);
                } else {
                    // User is ignoring unmapped fields, but log at debug level just in case
                    getLogger().debug("Did not map field '" + fieldName + "' to any column in the database\n"
                            + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
                }
            }
        }

        final Set<String> literalKeyColumnNames = new HashSet<>(keyColumnNames.size());
        for (String literalKeyColumnName : keyColumnNames) {
            if (settings.escapeColumnNames) {
                literalKeyColumnNames.add(tableSchema.getQuotedIdentifierString() + literalKeyColumnName + tableSchema.getQuotedIdentifierString());
            } else {
                literalKeyColumnNames.add(literalKeyColumnName);
            }
        }
        String sql = databaseAdapter.getUpsertStatement(tableName, usedColumnNames, literalKeyColumnNames);

        return new SqlAndIncludedColumns(sql, usedColumnIndices);
    }

    SqlAndIncludedColumns generateInsertIgnore(final RecordSchema recordSchema, final String tableName, final String updateKeys,
                                               final TableSchema tableSchema, final DMLSettings settings)
            throws IllegalArgumentException, SQLException, MalformedRecordException {

        checkValuesForRequiredColumns(recordSchema, tableSchema, settings);

        Set<String> keyColumnNames = getUpdateKeyColumnNames(tableName, updateKeys, tableSchema);
        normalizeKeyColumnNamesAndCheckForValues(recordSchema, updateKeys, settings, keyColumnNames);

        List<String> usedColumnNames = new ArrayList<>();
        List<Integer> usedColumnIndices = new ArrayList<>();

        List<String> fieldNames = recordSchema.getFieldNames();
        if (fieldNames != null) {
            int fieldCount = fieldNames.size();

            for (int i = 0; i < fieldCount; i++) {
                RecordField field = recordSchema.getField(i);
                String fieldName = field.getFieldName();

                final ColumnDescription desc = tableSchema.getColumns().get(ColumnDescription.normalizeColumnName(fieldName, settings.translateFieldNames));
                if (desc == null && !settings.ignoreUnmappedFields) {
                    throw new SQLDataException("Cannot map field '" + fieldName + "' to any column in the database\n"
                            + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
                }

                if (desc != null) {
                    if (settings.escapeColumnNames) {
                        usedColumnNames.add(tableSchema.getQuotedIdentifierString() + desc.getColumnName() + tableSchema.getQuotedIdentifierString());
                    } else {
                        usedColumnNames.add(desc.getColumnName());
                    }
                    usedColumnIndices.add(i);
                } else {
                    // User is ignoring unmapped fields, but log at debug level just in case
                    getLogger().debug("Did not map field '" + fieldName + "' to any column in the database\n"
                            + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
                }
            }
        }

        final Set<String> literalKeyColumnNames = new HashSet<>(keyColumnNames.size());
        for (String literalKeyColumnName : keyColumnNames) {
            if (settings.escapeColumnNames) {
                literalKeyColumnNames.add(tableSchema.getQuotedIdentifierString() + literalKeyColumnName + tableSchema.getQuotedIdentifierString());
            } else {
                literalKeyColumnNames.add(literalKeyColumnName);
            }
        }

        String sql = databaseAdapter.getInsertIgnoreStatement(tableName, usedColumnNames, literalKeyColumnNames);

        return new SqlAndIncludedColumns(sql, usedColumnIndices);
    }

    SqlAndIncludedColumns generateUpdate(final RecordSchema recordSchema, final String tableName, final String updateKeys,
                                         final TableSchema tableSchema, final DMLSettings settings)
            throws IllegalArgumentException, MalformedRecordException, SQLException {

        final Set<String> keyColumnNames = getUpdateKeyColumnNames(tableName, updateKeys, tableSchema);
        final Set<String> normalizedKeyColumnNames = normalizeKeyColumnNamesAndCheckForValues(recordSchema, updateKeys, settings, keyColumnNames);

        final StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("UPDATE ");
        sqlBuilder.append(tableName);

        // iterate over all the fields in the record, building the SQL statement by adding the column names
        List<String> fieldNames = recordSchema.getFieldNames();
        final List<Integer> includedColumns = new ArrayList<>();
        if (fieldNames != null) {
            sqlBuilder.append(" SET ");

            int fieldCount = fieldNames.size();
            AtomicInteger fieldsFound = new AtomicInteger(0);

            for (int i = 0; i < fieldCount; i++) {
                RecordField field = recordSchema.getField(i);
                String fieldName = field.getFieldName();

                final String normalizedColName = ColumnDescription.normalizeColumnName(fieldName, settings.translateFieldNames);
                final ColumnDescription desc = tableSchema.getColumns().get(ColumnDescription.normalizeColumnName(fieldName, settings.translateFieldNames));
                if (desc == null) {
                    if (!settings.ignoreUnmappedFields) {
                        throw new SQLDataException("Cannot map field '" + fieldName + "' to any column in the database\n"
                                + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
                    } else {
                        // User is ignoring unmapped fields, but log at debug level just in case
                        getLogger().debug("Did not map field '" + fieldName + "' to any column in the database\n"
                                + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
                        continue;
                    }
                }

                // Check if this column is an Update Key. If so, skip it for now. We will come
                // back to it after we finish the SET clause
                if (!normalizedKeyColumnNames.contains(normalizedColName)) {
                    if (fieldsFound.getAndIncrement() > 0) {
                        sqlBuilder.append(", ");
                    }

                    if (settings.escapeColumnNames) {
                        sqlBuilder.append(tableSchema.getQuotedIdentifierString())
                                .append(desc.getColumnName())
                                .append(tableSchema.getQuotedIdentifierString());
                    } else {
                        sqlBuilder.append(desc.getColumnName());
                    }

                    sqlBuilder.append(" = ?");
                    includedColumns.add(i);
                }
            }

            AtomicInteger whereFieldCount = new AtomicInteger(0);
            for (int i = 0; i < fieldCount; i++) {
                RecordField field = recordSchema.getField(i);
                String fieldName = field.getFieldName();
                boolean firstUpdateKey = true;

                final String normalizedColName = ColumnDescription.normalizeColumnName(fieldName, settings.translateFieldNames);
                final ColumnDescription desc = tableSchema.getColumns().get(ColumnDescription.normalizeColumnName(fieldName, settings.translateFieldNames));
                if (desc != null) {

                    // Check if this column is a Update Key. If so, add it to the WHERE clause
                    if (normalizedKeyColumnNames.contains(normalizedColName)) {

                        if (whereFieldCount.getAndIncrement() > 0) {
                            sqlBuilder.append(" AND ");
                        } else if (firstUpdateKey) {
                            // Set the WHERE clause based on the Update Key values
                            sqlBuilder.append(" WHERE ");
                            firstUpdateKey = false;
                        }

                        if (settings.escapeColumnNames) {
                            sqlBuilder.append(tableSchema.getQuotedIdentifierString())
                                    .append(desc.getColumnName())
                                    .append(tableSchema.getQuotedIdentifierString());
                        } else {
                            sqlBuilder.append(desc.getColumnName());
                        }
                        sqlBuilder.append(" = ?");
                        includedColumns.add(i);
                    }
                }
            }
        }
        return new SqlAndIncludedColumns(sqlBuilder.toString(), includedColumns);
    }

    SqlAndIncludedColumns generateDelete(final RecordSchema recordSchema, final String tableName, final TableSchema tableSchema, final DMLSettings settings)
            throws IllegalArgumentException, MalformedRecordException, SQLDataException {

        final Set<String> normalizedFieldNames = getNormalizedColumnNames(recordSchema, settings.translateFieldNames);
        for (final String requiredColName : tableSchema.getRequiredColumnNames()) {
            final String normalizedColName = ColumnDescription.normalizeColumnName(requiredColName, settings.translateFieldNames);
            if (!normalizedFieldNames.contains(normalizedColName)) {
                String missingColMessage = "Record does not have a value for the Required column '" + requiredColName + "'";
                if (settings.failUnmappedColumns) {
                    getLogger().error(missingColMessage);
                    throw new MalformedRecordException(missingColMessage);
                } else if (settings.warningUnmappedColumns) {
                    getLogger().warn(missingColMessage);
                }
            }
        }

        final StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("DELETE FROM ");
        sqlBuilder.append(tableName);

        // iterate over all of the fields in the record, building the SQL statement by adding the column names
        List<String> fieldNames = recordSchema.getFieldNames();
        final List<Integer> includedColumns = new ArrayList<>();
        if (fieldNames != null) {
            sqlBuilder.append(" WHERE ");
            int fieldCount = fieldNames.size();
            AtomicInteger fieldsFound = new AtomicInteger(0);

            for (int i = 0; i < fieldCount; i++) {

                RecordField field = recordSchema.getField(i);
                String fieldName = field.getFieldName();

                final ColumnDescription desc = tableSchema.getColumns().get(ColumnDescription.normalizeColumnName(fieldName, settings.translateFieldNames));
                if (desc == null && !settings.ignoreUnmappedFields) {
                    throw new SQLDataException("Cannot map field '" + fieldName + "' to any column in the database\n"
                            + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
                }

                if (desc != null) {
                    if (fieldsFound.getAndIncrement() > 0) {
                        sqlBuilder.append(" AND ");
                    }

                    String columnName;
                    if (settings.escapeColumnNames) {
                        columnName = tableSchema.getQuotedIdentifierString() + desc.getColumnName() + tableSchema.getQuotedIdentifierString();
                    } else {
                        columnName = desc.getColumnName();
                    }
                    // Need to build a null-safe construct for the WHERE clause, since we are using PreparedStatement and won't know if the values are null. If they are null,
                    // then the filter should be "column IS null" vs "column = null". Since we don't know whether the value is null, we can use the following construct (from NIFI-3742):
                    //   (column = ? OR (column is null AND ? is null))
                    sqlBuilder.append("(");
                    sqlBuilder.append(columnName);
                    sqlBuilder.append(" = ?");

                    // Only need null check if the column is nullable, otherwise the row wouldn't exist
                    if (desc.isNullable()) {
                        sqlBuilder.append(" OR (");
                        sqlBuilder.append(columnName);
                        sqlBuilder.append(" is null AND ? is null))");
                    } else {
                        sqlBuilder.append(")");
                    }
                    includedColumns.add(i);
                } else {
                    // User is ignoring unmapped fields, but log at debug level just in case
                    getLogger().debug("Did not map field '" + fieldName + "' to any column in the database\n"
                            + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
                }
            }

            if (fieldsFound.get() == 0) {
                throw new SQLDataException("None of the fields in the record map to the columns defined by the " + tableName + " table\n"
                        + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
            }
        }

        return new SqlAndIncludedColumns(sqlBuilder.toString(), includedColumns);
    }

    private void checkValuesForRequiredColumns(RecordSchema recordSchema, TableSchema tableSchema, DMLSettings settings) {
        final Set<String> normalizedFieldNames = getNormalizedColumnNames(recordSchema, settings.translateFieldNames);

        for (final String requiredColName : tableSchema.getRequiredColumnNames()) {
            final String normalizedColName = ColumnDescription.normalizeColumnName(requiredColName, settings.translateFieldNames);
            if (!normalizedFieldNames.contains(normalizedColName)) {
                String missingColMessage = "Record does not have a value for the Required column '" + requiredColName + "'";
                if (settings.failUnmappedColumns) {
                    getLogger().error(missingColMessage);
                    throw new IllegalArgumentException(missingColMessage);
                } else if (settings.warningUnmappedColumns) {
                    getLogger().warn(missingColMessage);
                }
            }
        }
    }

    private Set<String> getUpdateKeyColumnNames(String tableName, String updateKeys, TableSchema tableSchema) throws SQLIntegrityConstraintViolationException {
        final Set<String> updateKeyColumnNames;

        if (updateKeys == null) {
            updateKeyColumnNames = tableSchema.getPrimaryKeyColumnNames();
        } else {
            updateKeyColumnNames = new HashSet<>();
            for (final String updateKey : updateKeys.split(",")) {
                updateKeyColumnNames.add(updateKey.trim());
            }
        }

        if (updateKeyColumnNames.isEmpty()) {
            throw new SQLIntegrityConstraintViolationException("Table '" + tableName + "' not found or does not have a Primary Key and no Update Keys were specified");
        }

        return updateKeyColumnNames;
    }

    private Set<String> normalizeKeyColumnNamesAndCheckForValues(RecordSchema recordSchema, String updateKeys, DMLSettings settings, Set<String> updateKeyColumnNames)
            throws MalformedRecordException {
        // Create a Set of all normalized Update Key names, and ensure that there is a field in the record
        // for each of the Update Key fields.
        final Set<String> normalizedRecordFieldNames = getNormalizedColumnNames(recordSchema, settings.translateFieldNames);

        final Set<String> normalizedKeyColumnNames = new HashSet<>();
        for (final String updateKeyColumnName : updateKeyColumnNames) {
            String normalizedKeyColumnName = ColumnDescription.normalizeColumnName(updateKeyColumnName, settings.translateFieldNames);

            if (!normalizedRecordFieldNames.contains(normalizedKeyColumnName)) {
                String missingColMessage = "Record does not have a value for the " + (updateKeys == null ? "Primary" : "Update") + "Key column '" + updateKeyColumnName + "'";
                if (settings.failUnmappedColumns) {
                    getLogger().error(missingColMessage);
                    throw new MalformedRecordException(missingColMessage);
                } else if (settings.warningUnmappedColumns) {
                    getLogger().warn(missingColMessage);
                }
            }
            normalizedKeyColumnNames.add(normalizedKeyColumnName);
        }

        return normalizedKeyColumnNames;
    }

    private boolean isSupportsBatchUpdates(Connection connection) {
        try {
            return connection.getMetaData().supportsBatchUpdates();
        } catch (Exception ex) {
            getLogger().debug(String.format("Exception while testing if connection supportsBatchUpdates due to %s - %s",
                    ex.getClass().getName(), ex.getMessage()));
            return false;
        }
    }

    static class SchemaKey {
        private final String catalog;
        private final String schemaName;
        private final String tableName;

        public SchemaKey(final String catalog, final String schemaName, final String tableName) {
            this.catalog = catalog;
            this.schemaName = schemaName;
            this.tableName = tableName;
        }

        @Override
        public int hashCode() {
            int result = catalog != null ? catalog.hashCode() : 0;
            result = 31 * result + (schemaName != null ? schemaName.hashCode() : 0);
            result = 31 * result + tableName.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SchemaKey schemaKey = (SchemaKey) o;

            if (catalog != null ? !catalog.equals(schemaKey.catalog) : schemaKey.catalog != null) return false;
            if (schemaName != null ? !schemaName.equals(schemaKey.schemaName) : schemaKey.schemaName != null) return false;
            return tableName.equals(schemaKey.tableName);
        }
    }

    /**
     * A holder class for a SQL prepared statement and a BitSet indicating which columns are being updated (to determine which values from the record to set on the statement)
     * A value of null for getIncludedColumns indicates that all columns/fields should be included.
     */
    static class SqlAndIncludedColumns {
        private final String sql;
        private final List<Integer> fieldIndexes;

        /**
         * Constructor
         *
         * @param sql          The prepared SQL statement (including parameters notated by ? )
         * @param fieldIndexes A List of record indexes. The index of the list is the location of the record field in the SQL prepared statement
         */
        public SqlAndIncludedColumns(final String sql, final List<Integer> fieldIndexes) {
            this.sql = sql;
            this.fieldIndexes = fieldIndexes;
        }

        public String getSql() {
            return sql;
        }

        public List<Integer> getFieldIndexes() {
            return fieldIndexes;
        }
    }

    static class PreparedSqlAndColumns {
        private final SqlAndIncludedColumns sqlAndIncludedColumns;
        private final PreparedStatement preparedStatement;

        public PreparedSqlAndColumns(final SqlAndIncludedColumns sqlAndIncludedColumns, final PreparedStatement preparedStatement) {
            this.sqlAndIncludedColumns = sqlAndIncludedColumns;
            this.preparedStatement = preparedStatement;
        }

        public SqlAndIncludedColumns getSqlAndIncludedColumns() {
            return sqlAndIncludedColumns;
        }

        public PreparedStatement getPreparedStatement() {
            return preparedStatement;
        }
    }

    private static class RecordPathStatementType implements Function<Record, String> {
        private final RecordPath recordPath;

        public RecordPathStatementType(final RecordPath recordPath) {
            this.recordPath = recordPath;
        }

        @Override
        public String apply(final Record record) {
            final RecordPathResult recordPathResult = recordPath.evaluate(record);
            final List<FieldValue> resultList = recordPathResult.getSelectedFields().distinct().collect(Collectors.toList());
            if (resultList.isEmpty()) {
                throw new ProcessException("Evaluated RecordPath " + recordPath.getPath() + " against Record but got no results");
            }

            if (resultList.size() > 1) {
                throw new ProcessException("Evaluated RecordPath " + recordPath.getPath() + " against Record and received multiple distinct results (" + resultList + ")");
            }

            final String resultValue = String.valueOf(resultList.get(0).getValue()).toUpperCase();
            switch (resultValue) {
                case INSERT_TYPE:
                case UPDATE_TYPE:
                case DELETE_TYPE:
                case UPSERT_TYPE:
                    return resultValue;
            }

            throw new ProcessException("Evaluated RecordPath " + recordPath.getPath() + " against Record to determine Statement Type but found invalid value: " + resultValue);
        }
    }

    static class DMLSettings {
        private final boolean translateFieldNames;
        private final boolean ignoreUnmappedFields;

        // Is the unmatched column behaviour fail or warning?
        private final boolean failUnmappedColumns;
        private final boolean warningUnmappedColumns;

        // Escape column names?
        private final boolean escapeColumnNames;

        // Quote table name?
        private final boolean quoteTableName;

        DMLSettings(ProcessContext context) {
            translateFieldNames = context.getProperty(TRANSLATE_FIELD_NAMES).asBoolean();
            ignoreUnmappedFields = IGNORE_UNMATCHED_FIELD.getValue().equalsIgnoreCase(context.getProperty(UNMATCHED_FIELD_BEHAVIOR).getValue());

            failUnmappedColumns = FAIL_UNMATCHED_COLUMN.getValue().equalsIgnoreCase(context.getProperty(UNMATCHED_COLUMN_BEHAVIOR).getValue());
            warningUnmappedColumns = WARNING_UNMATCHED_COLUMN.getValue().equalsIgnoreCase(context.getProperty(UNMATCHED_COLUMN_BEHAVIOR).getValue());

            escapeColumnNames = context.getProperty(QUOTE_IDENTIFIERS).asBoolean();
            quoteTableName = context.getProperty(QUOTE_TABLE_IDENTIFIER).asBoolean();
        }
    }

}
