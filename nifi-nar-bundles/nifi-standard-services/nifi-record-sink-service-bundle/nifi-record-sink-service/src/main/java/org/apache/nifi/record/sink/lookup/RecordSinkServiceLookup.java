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
package org.apache.nifi.record.sink.lookup;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.record.sink.RecordSinkService;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.service.lookup.AbstractSingleAttributeBasedControllerServiceLookup;

import java.io.IOException;
import java.util.Map;


@Tags({"record", "sink", "lookup"})
@CapabilityDescription("Cung cấp một RecordSinkService có thể được sử dụng để chọn động một RecordSinkService khác. Dịch vụ này " +
        "yêu cầu một thuộc tính tên là 'record.sink.name' được truyền vào khi yêu cầu kết nối, và sẽ ném ngoại lệ " +
        "nếu thuộc tính này không có. Giá trị của 'record.sink.name' sẽ được sử dụng để chọn RecordSinkService đã được " +
        "đăng ký với tên đó. Điều này cho phép nhiều RecordSinkService được định nghĩa và đăng ký, sau đó được chọn " +
        "động tại thời điểm chạy bằng cách gán thuộc tính 'record.sink.name' thích hợp cho FlowFiles. Lưu ý rằng controller service " +
        "này không được thiết kế để sử dụng trong các reporting task sử dụng các instance của RecordSinkService, chẳng hạn như QueryNiFiReportingTask.")
@DynamicProperty(
        name = "Tên để đăng ký RecordSinkService được chỉ định",
        value = "RecordSinkService",
        description = "Nếu thuộc tính '" + RecordSinkServiceLookup.RECORD_SINK_NAME_ATTRIBUTE + "' chứa " +
                      "tên của dynamic property, thì RecordSinkService (đăng ký trong giá trị) sẽ được chọn.",
        expressionLanguageScope = ExpressionLanguageScope.NONE
)
public class RecordSinkServiceLookup
        extends AbstractSingleAttributeBasedControllerServiceLookup<RecordSinkService> implements RecordSinkService {

    public static final String RECORD_SINK_NAME_ATTRIBUTE = "record.sink.name";

    RecordSinkService recordSinkService;

    @Override
    protected String getLookupAttribute() {
        return RECORD_SINK_NAME_ATTRIBUTE;
    }

    @Override
    public Class<RecordSinkService> getServiceType() {
        return RecordSinkService.class;
    }

    @Override
    public WriteResult sendData(RecordSet recordSet, Map<String, String> attributes, boolean sendZeroResults) throws IOException {
        try {
            RecordSinkService recordSink = lookupService(attributes);
            if (recordSinkService != recordSink) {
                // Save for later reset(), and do a reset now since it has changed
                recordSinkService = recordSink;
                recordSinkService.reset();
            }
            return recordSinkService.sendData(recordSet, attributes, sendZeroResults);
        } catch (ProcessException pe) {
            // Lookup was unsuccessful, wrap the exception in an IOException to honor the contract
            throw new IOException(pe);
        }
    }

    @Override
    public void reset() {
        // By convention, calling reset() before sendData should be a no-op, so we can just delegate the reset() call after sendData() has been called once
        if (recordSinkService != null) {
            recordSinkService.reset();
        }
    }
}
