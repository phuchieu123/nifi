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

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.record.sink.RecordSinkService;
import org.apache.nifi.record.sink.RetryableIOException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@EventDriven
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"record", "put", "sink"})
@CapabilityDescription("Bộ xử lý PutRecord sử dụng một RecordReader được chỉ định để đầu vào (có thể là nhiều) bản ghi từ một tệp luồng đến, và gửi chúng "
        + "đến một đích được chỉ định bởi Dịch vụ Đích Bản ghi (tức là bộ chứa bản ghi).")
public class PutRecord extends AbstractProcessor {

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("put-record-reader")
            .displayName("Bộ đọc bản ghi")
            .description("Chỉ định Dịch vụ Bộ điều khiển sẽ được sử dụng để đọc dữ liệu đến")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    public static final PropertyDescriptor RECORD_SINK = new PropertyDescriptor.Builder()
            .name("put-record-sink")
            .displayName("Dịch vụ Đích Bản ghi")
            .description("Chỉ định Dịch vụ Bộ điều khiển sẽ được sử dụng để viết các bản ghi kết quả truy vấn đến một số đích.")
            .identifiesControllerService(RecordSinkService.class)
            .required(true)
            .build();

    public static final PropertyDescriptor INCLUDE_ZERO_RECORD_RESULTS = new PropertyDescriptor.Builder()
            .name("put-record-include-zero-record-results")
            .displayName("Bao gồm Kết quả Không Bản ghi")
            .description("Nếu không có bản ghi nào được đọc từ FlowFile đến, thuộc tính này chỉ định liệu có truyền tải một bộ bản ghi trống hay không. FlowFile ban đầu "
                    + "sẽ vẫn được định tuyến đến thành công, nhưng nếu không có truyền tải nào xảy ra, sẽ không có sự kiện SEND provenance nào được tạo.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    // Mối quan hệ
    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFile ban đầu sẽ được định tuyến đến mối quan hệ này nếu các bản ghi được truyền tải thành công")
            .build();

    static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("FlowFile ban đầu được định tuyến đến mối quan hệ này nếu các bản ghi không thể được truyền tải nhưng thử lại hoạt động có thể thành công")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Một FlowFile được định tuyến đến mối quan hệ này nếu các bản ghi không thể được truyền tải và thử lại hoạt động cũng sẽ thất bại")
            .build();

    private static final List<PropertyDescriptor> properties;
    private static final Set<Relationship> relationships;

    private volatile RecordSinkService recordSinkService;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(RECORD_READER);
        props.add(RECORD_SINK);
        props.add(INCLUDE_ZERO_RECORD_RESULTS);
        properties = Collections.unmodifiableList(props);

        final Set<Relationship> r = new HashSet<>();
        r.add(REL_SUCCESS);
        r.add(REL_FAILURE);
        r.add(REL_RETRY);
        relationships = Collections.unmodifiableSet(r);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        recordSinkService = context.getProperty(RECORD_SINK).asControllerService(RecordSinkService.class);
        recordSinkService.reset();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final StopWatch stopWatch = new StopWatch(true);

        RecordSet recordSet;
        try (final InputStream in = session.read(flowFile)) {

            final RecordReaderFactory recordParserFactory = context.getProperty(RECORD_READER)
                    .asControllerService(RecordReaderFactory.class);
            final RecordReader recordParser = recordParserFactory.createRecordReader(flowFile, in, getLogger());
            recordSet = recordParser.createRecordSet();

            final boolean transmitZeroRecords = context.getProperty(INCLUDE_ZERO_RECORD_RESULTS).asBoolean();
            final WriteResult writeResult = recordSinkService.sendData(recordSet, new HashMap<>(flowFile.getAttributes()), transmitZeroRecords);
            String recordSinkURL = writeResult.getAttributes().get("record.sink.url");
            if (StringUtils.isEmpty(recordSinkURL)) {
                recordSinkURL = "unknown://";
            }

            final long transmissionMillis = stopWatch.getElapsed(TimeUnit.MILLISECONDS);
            // Only record provenance if we sent any records
            if (writeResult.getRecordCount() > 0 || transmitZeroRecords) {
                session.getProvenanceReporter().send(flowFile, recordSinkURL, transmissionMillis);
            }

        } catch (RetryableIOException rioe) {
            getLogger().warn("Error during transmission of records due to {}, routing to retry", rioe.getMessage(), rioe);
            session.transfer(flowFile, REL_RETRY);
            return;
        } catch (SchemaNotFoundException snfe) {
            throw new ProcessException("Error determining schema of flowfile records: " + snfe.getMessage(), snfe);
        } catch (MalformedRecordException e) {
            getLogger().error("Error reading records from {} due to {}, routing to failure", flowFile, e.getMessage(), e);
            session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        } catch (IOException ioe) {
            // The cause might be a MalformedRecordException (RecordReader wraps it in an IOException), send to failure in that case
            if (ioe.getCause() instanceof MalformedRecordException) {
                getLogger().error("Error reading records from {} due to {}, routing to failure", flowFile, ioe.getMessage(), ioe);
                session.penalize(flowFile);
                session.transfer(flowFile, REL_FAILURE);
                return;
            }
            throw new ProcessException("Error reading from flowfile input stream: " + ioe.getMessage(), ioe);
        } catch (Exception e) {
            getLogger().error("Error during transmission of records due to {}, routing to failure", e.getMessage(), e);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }
        session.transfer(flowFile, REL_SUCCESS);
    }
}
