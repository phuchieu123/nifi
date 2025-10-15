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

package org.apache.nifi.processors.airtable;

import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_COUNT;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_ID;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_INDEX;
import static org.apache.nifi.processors.airtable.service.AirtableRestService.API_V0_BASE_URL;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.configuration.DefaultSettings;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.airtable.parse.AirtableRetrieveTableResult;
import org.apache.nifi.processors.airtable.parse.AirtableTableRetriever;
import org.apache.nifi.processors.airtable.service.AirtableGetRecordsParameters;
import org.apache.nifi.processors.airtable.service.AirtableRestService;
import org.apache.nifi.processors.airtable.service.RateLimitExceededException;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;

@PrimaryNodeOnly
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@TriggerSerially
@TriggerWhenEmpty
@Tags({"airtable", "query", "database"})
@CapabilityDescription("Truy vấn các bản ghi từ một bảng Airtable. Các bản ghi được truy xuất tăng dần dựa trên thời gian sửa đổi cuối cùng của các bản ghi."
        + " Các bản ghi cũng có thể được lọc thêm bằng cách đặt thuộc tính 'Bộ lọc tùy chỉnh' hỗ trợ các công thức do API Airtable cung cấp."
        + " Bộ xử lý này chỉ được thiết kế để chạy trên Nút chính.")
@Stateful(scopes = Scope.CLUSTER, description = "Thời gian của truy vấn thành công cuối cùng được lưu trữ để cho phép tải tăng dần."
        + " Truy vấn ban đầu trả về tất cả các bản ghi trong bảng và mỗi truy vấn tiếp theo sẽ lọc các bản ghi theo thời gian sửa đổi cuối cùng của chúng."
        + " Nói cách khác, nếu một bản ghi được cập nhật sau truy vấn thành công cuối cùng, chỉ những bản ghi được cập nhật mới được trả về trong truy vấn tiếp theo."
        + " Trạng thái được lưu trữ trên toàn cụm, vì vậy Bộ xử lý này chỉ có thể chạy trên Nút chính và nếu một Nút chính mới được chọn,"
        + " nút mới có thể tiếp tục từ nơi nút trước đó đã dừng lại mà không sao chép dữ liệu.")
@WritesAttributes({
        @WritesAttribute(attribute = "record.count", description = "Đặt số lượng bản ghi trong FlowFile."),
        @WritesAttribute(attribute = "fragment.identifier", description = "Nếu 'Số bản ghi tối đa mỗi FlowFile' được đặt thì tất cả các FlowFile từ cùng một tập kết quả truy vấn "
                + "sẽ có cùng giá trị cho thuộc tính fragment.identifier. Điều này sau đó có thể được sử dụng để tương quan các kết quả."),
        @WritesAttribute(attribute = "fragment.count", description = "Nếu 'Số bản ghi tối đa mỗi FlowFile' được đặt thì đây là tổng số "
                + "FlowFile được tạo ra bởi một ResultSet duy nhất. Điều này có thể được sử dụng kết hợp với "
                + "thuộc tính fragment.identifier để biết có bao nhiêu FlowFile thuộc cùng một ResultSet đến."),
        @WritesAttribute(attribute = "fragment.index", description = "Nếu 'Số bản ghi tối đa mỗi FlowFile' được đặt thì vị trí của FlowFile này trong danh sách các "
                + "FlowFile đi ra mà tất cả đều được lấy từ cùng một FlowFile tập kết quả. Điều này có thể "
                + "được sử dụng kết hợp với thuộc tính fragment.identifier để biết FlowFile nào bắt nguồn từ cùng một tập kết quả truy vấn và theo thứ tự "
                + "FlowFile nào đã được tạo ra"),
})
@DefaultSettings(yieldDuration = "15 sec")
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
public class QueryAirtableTable extends AbstractProcessor {

    static final PropertyDescriptor API_URL = new PropertyDescriptor.Builder()
            .name("api-url")
            .displayName("URL API")
            .description("URL cho API REST của Airtable bao gồm tên miền và đường dẫn đến API (ví dụ: https://api.airtable.com/v0).")
            .defaultValue(API_V0_BASE_URL)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .build();

    // Các khóa API đã không còn được dùng nữa, Airtable hiện cung cấp Token Truy cập Cá nhân thay thế.
    static final PropertyDescriptor PAT = new PropertyDescriptor.Builder()
            .name("api-key")
            .displayName("Token Truy cập Cá nhân")
            .description("Token Truy cập Cá nhân (PAT) để sử dụng trong các truy vấn. Nên được tạo trên trang tài khoản của Airtable.")
            .required(true)
            .sensitive(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor BASE_ID = new PropertyDescriptor.Builder()
            .name("base-id")
            .displayName("ID Base")
            .description("ID của base Airtable cần được truy vấn.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor TABLE_ID = new PropertyDescriptor.Builder()
            .name("table-id")
            .displayName("ID Bảng")
            .description("Tên hoặc ID của bảng Airtable cần được truy vấn.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor FIELDS = new PropertyDescriptor.Builder()
            .name("fields")
            .displayName("Các trường")
            .description("Danh sách các trường được phân tách bằng dấu phẩy để truy vấn từ bảng. Cả tên và ID của trường đều có thể được sử dụng.")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor CUSTOM_FILTER = new PropertyDescriptor.Builder()
            .name("custom-filter")
            .displayName("Bộ lọc tùy chỉnh")
            .description("Lọc các bản ghi bằng các công thức của Airtable.")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor QUERY_TIME_WINDOW_LAG = new PropertyDescriptor.Builder()
            .name("query-time-window-lag")
            .displayName("Độ trễ cửa sổ thời gian truy vấn")
            .description("Lượng độ trễ được áp dụng cho điểm cuối của cửa sổ thời gian truy vấn. Đặt thuộc tính này để tránh bỏ lỡ các bản ghi khi đồng hồ của máy cục bộ"
                    + " và đồng hồ của máy chủ Airtable không đồng bộ. Phải lớn hơn hoặc bằng 1 giây.")
            .defaultValue("3 s")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final PropertyDescriptor WEB_CLIENT_SERVICE_PROVIDER = new PropertyDescriptor.Builder()
            .name("web-client-service-provider")
            .displayName("Nhà cung cấp dịch vụ Web Client")
            .description("Nhà cung cấp dịch vụ Web Client để sử dụng cho các yêu cầu API REST của Airtable")
            .identifiesControllerService(WebClientServiceProvider.class)
            .required(true)
            .build();

    static final PropertyDescriptor QUERY_PAGE_SIZE = new PropertyDescriptor.Builder()
            .name("query-page-size")
            .displayName("Kích thước trang truy vấn")
            .description("Số lượng bản ghi được lấy trong một trang. Nên nằm trong khoảng từ 1 đến 100 (bao gồm).")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.createLongValidator(1, 100, true))
            .build();

    static final PropertyDescriptor MAX_RECORDS_PER_FLOWFILE = new PropertyDescriptor.Builder()
            .name("max-records-per-flowfile")
            .displayName("Số bản ghi tối đa mỗi FlowFile")
            .description("Số lượng bản ghi kết quả tối đa sẽ được bao gồm trong một FlowFile duy nhất. Điều này sẽ cho phép bạn chia nhỏ các tập kết quả rất lớn"
                    + " thành nhiều FlowFile. Nếu không có giá trị nào được chỉ định, thì tất cả các bản ghi sẽ được trả về trong một FlowFile duy nhất.")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Đối với các FlowFile được tạo ra từ một truy vấn thành công.")
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            API_URL,
            PAT,
            BASE_ID,
            TABLE_ID,
            FIELDS,
            CUSTOM_FILTER,
            QUERY_TIME_WINDOW_LAG,
            WEB_CLIENT_SERVICE_PROVIDER,
            QUERY_PAGE_SIZE,
            MAX_RECORDS_PER_FLOWFILE
    ));

    private static final Set<Relationship> RELATIONSHIPS = Collections.singleton(REL_SUCCESS);

    private static final String LAST_QUERY_TIME_WINDOW_END = "last_query_time_window_end";

    private volatile AirtableRestService airtableRestService;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final String apiUrl = context.getProperty(API_URL).evaluateAttributeExpressions().getValue();
        final String pat = context.getProperty(PAT).getValue();
        final String baseId = context.getProperty(BASE_ID).evaluateAttributeExpressions().getValue();
        final String tableId = context.getProperty(TABLE_ID).evaluateAttributeExpressions().getValue();
        final WebClientServiceProvider webClientServiceProvider = context.getProperty(WEB_CLIENT_SERVICE_PROVIDER).asControllerService(WebClientServiceProvider.class);
        airtableRestService = new AirtableRestService(webClientServiceProvider, apiUrl, pat, baseId, tableId);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final Integer maxRecordsPerFlowFile = context.getProperty(MAX_RECORDS_PER_FLOWFILE).evaluateAttributeExpressions().asInteger();
        final Long queryTimeWindowLagSeconds = context.getProperty(QUERY_TIME_WINDOW_LAG).evaluateAttributeExpressions().asTimePeriod(TimeUnit.SECONDS);

        final StateMap state;
        try {
            state = session.getState(Scope.CLUSTER);
        } catch (IOException e) {
            throw new ProcessException("Failed to get cluster state", e);
        }

        final String lastRecordFetchDateTime = state.get(LAST_QUERY_TIME_WINDOW_END);
        final String currentRecordFetchDateTime = OffsetDateTime.now()
                .minusSeconds(queryTimeWindowLagSeconds)
                .truncatedTo(ChronoUnit.SECONDS)
                .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);

        final AirtableGetRecordsParameters getRecordsParameters = buildGetRecordsParameters(context, lastRecordFetchDateTime, currentRecordFetchDateTime);
        final AirtableRetrieveTableResult retrieveTableResult;
        try {
            final AirtableTableRetriever tableRetriever = new AirtableTableRetriever(airtableRestService, getRecordsParameters, maxRecordsPerFlowFile);
            retrieveTableResult = tableRetriever.retrieveAll(session);
        } catch (IOException e) {
            throw new ProcessException("Failed to read Airtable records", e);
        } catch (RateLimitExceededException e) {
            context.yield();
            throw new ProcessException("Airtable REST API rate limit exceeded while reading records", e);
        }

        final Map<String, String> newState = new HashMap<>(state.toMap());
        newState.put(LAST_QUERY_TIME_WINDOW_END, currentRecordFetchDateTime);
        try {
            session.setState(newState, Scope.CLUSTER);
        } catch (IOException e) {
            throw new ProcessException("Failed to update cluster state", e);
        }

        final List<FlowFile> flowFiles = retrieveTableResult.getFlowFiles();
        if (flowFiles.isEmpty()) {
            context.yield();
            return;
        }

        if (maxRecordsPerFlowFile != null) {
            addFragmentAttributesToFlowFiles(session, flowFiles);
        }
        transferFlowFiles(session, flowFiles, retrieveTableResult.getTotalRecordCount());
    }

    private AirtableGetRecordsParameters buildGetRecordsParameters(final ProcessContext context,
            final String lastRecordFetchTime,
            final String nowDateTimeString) {
        Objects.requireNonNull(context);
        Objects.requireNonNull(nowDateTimeString);

        final String fieldsProperty = context.getProperty(FIELDS).evaluateAttributeExpressions().getValue();
        final String customFilter = context.getProperty(CUSTOM_FILTER).evaluateAttributeExpressions().getValue();
        final Integer pageSize = context.getProperty(QUERY_PAGE_SIZE).evaluateAttributeExpressions().asInteger();

        final AirtableGetRecordsParameters.Builder getRecordsParametersBuilder = new AirtableGetRecordsParameters.Builder();
        if (lastRecordFetchTime != null) {
            getRecordsParametersBuilder.modifiedAfter(lastRecordFetchTime);
        }
        getRecordsParametersBuilder.modifiedBefore(nowDateTimeString);
        if (fieldsProperty != null) {
            getRecordsParametersBuilder.fields(Arrays.stream(fieldsProperty.split(",")).map(String::trim).collect(Collectors.toList()));
        }
        getRecordsParametersBuilder.customFilter(customFilter);
        if (pageSize != null) {
            getRecordsParametersBuilder.pageSize(pageSize);
        }

        return getRecordsParametersBuilder.build();
    }

    private void addFragmentAttributesToFlowFiles(final ProcessSession session, final List<FlowFile> flowFiles) {
        final String fragmentIdentifier = UUID.randomUUID().toString();
        for (int i = 0; i < flowFiles.size(); i++) {
            final Map<String, String> fragmentAttributes = new HashMap<>();
            fragmentAttributes.put(FRAGMENT_ID.key(), fragmentIdentifier);
            fragmentAttributes.put(FRAGMENT_INDEX.key(), String.valueOf(i));
            fragmentAttributes.put(FRAGMENT_COUNT.key(), String.valueOf(flowFiles.size()));

            flowFiles.set(i, session.putAllAttributes(flowFiles.get(i), fragmentAttributes));
        }
    }

    private void transferFlowFiles(final ProcessSession session, final List<FlowFile> flowFiles, final int totalRecordCount) {
        final String transitUri = airtableRestService.createUriBuilder().build().toString();
        for (final FlowFile flowFile : flowFiles) {
            session.getProvenanceReporter().receive(flowFile, transitUri);
            session.transfer(flowFile, REL_SUCCESS);
        }
        session.adjustCounter("Records Processed", totalRecordCount, false);
        final String flowFilesAsString = flowFiles.stream().map(FlowFile::toString).collect(Collectors.joining(", ", "[", "]"));
        getLogger().debug("Transferred FlowFiles [{}] Records [{}]", flowFilesAsString, totalRecordCount);
    }
}
