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

package org.apache.nifi.minifi.commons.schema.common;

import java.util.Collections;
import java.util.Map;

public class CommonPropertyKeys {
  public static final String CORE_PROPS_KEY = "Thuộc tính lõi";
    public static final String FLOWFILE_REPO_KEY = "Kho lưu trữ FlowFile";
    public static final String SWAP_PROPS_KEY = "Hoán đổi";
    public static final String FLOW_CONTROLLER_PROPS_KEY = "Bộ điều khiển luồng";
    public static final String CONTENT_REPO_KEY = "Kho lưu trữ nội dung";
    public static final String COMPONENT_STATUS_REPO_KEY = "Kho lưu trữ trạng thái thành phần";
    public static final String SECURITY_PROPS_KEY = "Thuộc tính bảo mật";
    public static final String SENSITIVE_PROPS_KEY = "Thuộc tính nhạy cảm";
    public static final String PROCESSORS_KEY = "Bộ xử lý";
    public static final String CONNECTIONS_KEY = "Kết nối";
    public static final String PROVENANCE_REPORTING_KEY = "Báo cáo nguồn gốc";
    public static final String GENERAL_REPORTING_KEY = "Tác vụ báo cáo";
    public static final String REMOTE_PROCESS_GROUPS_KEY = "Nhóm xử lý từ xa";
    public static final String INPUT_PORTS_KEY = "Cổng đầu vào";
    public static final String OUTPUT_PORTS_KEY = "Cổng đầu ra";
    public static final String CONTROLLER_SERVICES_KEY = "Dịch vụ điều khiển";
    public static final String FUNNELS_KEY = "Phễu";
    public static final String PROVENANCE_REPO_KEY = "Kho lưu trữ nguồn gốc";
    public static final String NIFI_PROPERTIES_OVERRIDES_KEY = "Ghi đè thuộc tính Life";

    public static final String NAME_KEY = "name";
    public static final String COMMENT_KEY = "comment";
    public static final String ALWAYS_SYNC_KEY = "always sync";
    public static final String YIELD_PERIOD_KEY = "yield period";
    public static final String MAX_CONCURRENT_THREADS_KEY = "max concurrent threads";
    public static final String MAX_CONCURRENT_TASKS_KEY = "max concurrent tasks";
    public static final String ID_KEY = "id";
    public static final String SCHEDULING_STRATEGY_KEY = "scheduling strategy";
    public static final String SCHEDULING_PERIOD_KEY = "scheduling period";
    public static final String USE_COMPRESSION_KEY = "use compression";
    public static final String PROPERTIES_KEY = "Properties";
    public static final String CLASS_KEY = "class";
    public static final String TYPE_KEY = "type";
    public static final String ANNOTATION_DATA_KEY = "annotation data";

    public static final Map<String, Object> DEFAULT_PROPERTIES = Collections.emptyMap();
    private CommonPropertyKeys() {
    }
}
