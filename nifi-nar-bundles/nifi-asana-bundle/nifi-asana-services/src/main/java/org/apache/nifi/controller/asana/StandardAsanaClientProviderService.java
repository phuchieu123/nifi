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
package org.apache.nifi.controller.asana;

import static org.apache.nifi.controller.asana.StandardAsanaClient.ASANA_CLIENT_OPTION_BASE_URL;

import com.asana.Client;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
@CapabilityDescription("Dịch vụ chung để xác thực với Asana và làm việc trên không gian làm việc được chỉ định.")
@Tags({"asana", "service", "authentication"})
public class StandardAsanaClientProviderService extends AbstractControllerService implements AsanaClientProviderService {

    protected static final String ASANA_API_URL = "asana-api-url";
    protected static final String ASANA_PERSONAL_ACCESS_TOKEN = "asana-personal-access-token";
    protected static final String ASANA_WORKSPACE_NAME = "asana-workspace-name";

    protected static final PropertyDescriptor PROP_ASANA_API_BASE_URL = new PropertyDescriptor.Builder()
            .name(ASANA_API_URL)
            .displayName("URL API")
            .description("URL cơ sở của API Asana. Để nguyên giá trị mặc định, trừ khi bạn có phiên bản Asana riêng của mình "
                    + "chạy trên một URL khác. (điển hình cho các cài đặt tại chỗ)")
            .required(true)
            .defaultValue(Client.DEFAULTS.get(ASANA_CLIENT_OPTION_BASE_URL).toString())
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    protected static final PropertyDescriptor PROP_ASANA_PERSONAL_ACCESS_TOKEN = new PropertyDescriptor.Builder()
            .name(ASANA_PERSONAL_ACCESS_TOKEN)
            .displayName("Mã thông báo Truy cập Cá nhân")
            .description("Tương tự như nhập tên người dùng/mật khẩu của bạn vào trang web, khi bạn truy cập "
                    + "dữ liệu Asana của mình qua API, bạn cần xác thực. Mã thông báo Truy cập Cá nhân (PAT) "
                    + "là một cơ chế xác thực để truy cập API. Bạn có thể tạo một PAT từ "
                    + "bảng điều khiển nhà phát triển Asana. Tham khảo Hướng dẫn Khởi động Nhanh Xác thực Asana để biết chi tiết "
                    + "hướng dẫn bắt đầu.")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    protected static final PropertyDescriptor PROP_ASANA_WORKSPACE_NAME = new PropertyDescriptor.Builder()
            .name(ASANA_WORKSPACE_NAME)
            .displayName("Không gian làm việc")
            .description("Chỉ định không gian làm việc Asana sẽ sử dụng. Phân biệt chữ hoa chữ thường. "
                    + "Không gian làm việc là đơn vị tổ chức cấp cao nhất trong Asana. Tất cả các dự án và tác vụ "
                    + "có không gian làm việc được liên kết. Một tổ chức là một loại không gian làm việc đặc biệt đại diện cho "
                    + "một công ty. Trong một tổ chức, bạn có thể nhóm các dự án của mình thành các nhóm.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    protected static final List<PropertyDescriptor> DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
            PROP_ASANA_API_BASE_URL,
            PROP_ASANA_PERSONAL_ACCESS_TOKEN,
            PROP_ASANA_WORKSPACE_NAME
    ));

    private volatile String personalAccessToken;
    private volatile String workspaceName;
    private volatile String baseUrl;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @OnEnabled
    public synchronized void onEnabled(final ConfigurationContext context) {
        personalAccessToken = context.getProperty(PROP_ASANA_PERSONAL_ACCESS_TOKEN).getValue();
        workspaceName = context.getProperty(PROP_ASANA_WORKSPACE_NAME).getValue();
        baseUrl = context.getProperty(PROP_ASANA_API_BASE_URL).getValue();
    }

    @Override
    public synchronized AsanaClient createClient() {
        return new StandardAsanaClient(personalAccessToken, workspaceName, baseUrl);
    }
}
