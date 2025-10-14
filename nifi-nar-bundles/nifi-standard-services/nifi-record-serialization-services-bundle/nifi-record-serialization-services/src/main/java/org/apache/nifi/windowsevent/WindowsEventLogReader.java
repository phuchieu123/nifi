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
package org.apache.nifi.windowsevent;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.RecordFieldType;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;


@Tags({"xml", "windows", "event", "log", "record", "reader", "parser"})
@CapabilityDescription("Đọc dữ liệu Windows Event Log dưới dạng nội dung XML được tạo bởi ConsumeWindowsEventLog, ParseEvtx, v.v. (xem Chi tiết bổ sung) và tạo (các) đối tượng Record. Nếu "
        + "thẻ gốc của XML đầu vào là 'Events', nội dung con dự kiến sẽ là một chuỗi các thẻ 'Event', mỗi thẻ sẽ tạo thành một bản ghi duy nhất. Nếu thẻ gốc là 'Event', "
        + "nội dung dự kiến sẽ là một 'Event' duy nhất và do đó là một bản ghi duy nhất. Không có thẻ gốc nào khác hợp lệ. Hiện tại chỉ hỗ trợ các sự kiện thuộc loại 'System'.")
public class WindowsEventLogReader extends AbstractControllerService implements RecordReaderFactory {

    private static final String DATE_FORMAT = RecordFieldType.DATE.getDefaultFormat();
    private static final String TIME_FORMAT = RecordFieldType.TIME.getDefaultFormat();
    private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS"; // The timestamps have nanoseconds but need a SimpleDateFormat string here

    @Override
    public RecordReader createRecordReader(Map<String, String> variables, InputStream in, long inputLength, ComponentLog logger)
            throws MalformedRecordException, IOException, SchemaNotFoundException {
        return new WindowsEventLogRecordReader(in, DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT, logger);
    }
}
