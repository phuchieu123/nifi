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
package org.apache.nifi.processors.stateful.analysis;

import org.apache.commons.math3.stat.descriptive.moment.Variance;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.processors.stateful.analysis.AttributeRollingWindow.ROLLING_WINDOW_COUNT_KEY;
import static org.apache.nifi.processors.stateful.analysis.AttributeRollingWindow.ROLLING_WINDOW_MEAN_KEY;
import static org.apache.nifi.processors.stateful.analysis.AttributeRollingWindow.ROLLING_WINDOW_STDDEV_KEY;
import static org.apache.nifi.processors.stateful.analysis.AttributeRollingWindow.ROLLING_WINDOW_VALUE_KEY;
import static org.apache.nifi.processors.stateful.analysis.AttributeRollingWindow.ROLLING_WINDOW_VARIANCE_KEY;

@TriggerSerially
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"Attribute Expression Language", "state", "data science", "rolling", "window"})
@CapabilityDescription("Theo dõi một cửa sổ cuộn (Rolling Window) dựa trên việc đánh giá biểu thức Ngôn ngữ Biểu thức (Expression Language) trên từng FlowFile và thêm giá trị đó vào trạng thái của bộ xử lý. Mỗi FlowFile sẽ được xuất ra " +
        "với số lượng FlowFile và tổng giá trị tổng hợp của các giá trị được xử lý trong cửa sổ thời gian hiện tại.")
@WritesAttributes({
        @WritesAttribute(attribute = ROLLING_WINDOW_VALUE_KEY, description = "Giá trị của cửa sổ cuộn (tổng của tất cả các giá trị được lưu trữ)."),
        @WritesAttribute(attribute = ROLLING_WINDOW_COUNT_KEY, description = "Số lượng FlowFile được ghi nhận trong cửa sổ cuộn."),
        @WritesAttribute(attribute = ROLLING_WINDOW_MEAN_KEY, description = "Giá trị trung bình của các FlowFile trong cửa sổ cuộn."),
        @WritesAttribute(attribute = ROLLING_WINDOW_VARIANCE_KEY, description = "Phương sai của các FlowFile trong cửa sổ cuộn."),
        @WritesAttribute(attribute = ROLLING_WINDOW_STDDEV_KEY, description = "Độ lệch chuẩn (căn bậc hai của phương sai) của các FlowFile trong cửa sổ cuộn.")
})
@Stateful(scopes = {Scope.LOCAL}, description = "Lưu trữ các giá trị sao lưu cửa sổ cuộn. Điều này bao gồm lưu trữ các giá trị riêng lẻ và dấu thời gian của chúng hoặc các lô giá trị và " +
        "đếm.")
public class AttributeRollingWindow extends AbstractProcessor {

    public static final String COUNT_KEY = "count";
    public static final String ROLLING_WINDOW_VALUE_KEY = "rolling_window_value";
    public static final String ROLLING_WINDOW_COUNT_KEY = "rolling_window_count";
    public static final String ROLLING_WINDOW_MEAN_KEY = "rolling_window_mean";
    public static final String ROLLING_WINDOW_VARIANCE_KEY = "rolling_window_variance";
    public static final String ROLLING_WINDOW_STDDEV_KEY = "rolling_window_stddev";

    public static final String CURRENT_MICRO_BATCH_STATE_TS_KEY = "start_curr_batch_ts";
    public static final String BATCH_APPEND_KEY = "_batch";
    public static final String COUNT_APPEND_KEY = "_count";
    public static final int COUNT_APPEND_KEY_LENGTH = 6;

    static final PropertyDescriptor VALUE_TO_TRACK = new PropertyDescriptor.Builder()
        .displayName("Giá trị cần theo dõi")
        .name("Value to track")
        .description("Biểu thức được dùng để đánh giá trên từng FlowFile. Kết quả của biểu thức này sẽ được thêm vào giá trị của cửa sổ cuộn (rolling window).")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
        .required(true)
        .build();

static final PropertyDescriptor TIME_WINDOW = new PropertyDescriptor.Builder()
        .displayName("Khoảng thời gian cửa sổ")
        .name("Time window")
        .description("Khoảng thời gian được sử dụng để tính toán cửa sổ cuộn.")
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .required(true)
        .build();

static final PropertyDescriptor SUB_WINDOW_LENGTH = new PropertyDescriptor.Builder()
        .displayName("Độ dài cửa sổ con")
        .name("Sub-window length")
        .description("Khi được thiết lập, các giá trị sẽ được gom thành các cửa sổ con có độ dài xác định. Điều này cho phép thiết lập tổng cửa sổ dài hơn nhưng giảm độ chính xác. Nếu không được thiết lập (hoặc là 0) thì mỗi giá trị sẽ được lưu cùng dấu thời gian khi nhận. Sau khi khoảng thời gian trong "
                + TIME_WINDOW.getDisplayName() + " trôi qua, giá trị đó sẽ bị loại bỏ. Nếu được thiết lập, các giá trị sẽ được gom nhóm lại theo mỗi khoảng X thời gian (với X là giá trị được cấu hình cho thuộc tính này) và bị xóa cùng lúc.")
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .required(false)
        .build();



    private final Set<Relationship> relationships;
    private final List<PropertyDescriptor> properties;
    private Long timeWindow;
    private Long microBatchTime;
    private static final Scope SCOPE = Scope.LOCAL;

    // relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
        .description("Tất cả các FlowFile được xử lý thành công sẽ được chuyển đến đây.")
        .name("success")
        .build();

    public static final Relationship REL_FAILED_SET_STATE = new Relationship.Builder()
        .name("set state fail")
        .description("Khi không thể lưu trạng thái trong quá trình xử lý FlowFile, FlowFile sẽ được chuyển đến đây.")
        .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("Khi FlowFile bị lỗi do nguyên nhân khác ngoài lỗi lưu trạng thái, nó sẽ được chuyển đến đây.")
        .build();


    {
        final Set<Relationship> relationshipSet = new HashSet<>();
        relationshipSet.add(REL_SUCCESS);
        relationshipSet.add(REL_FAILED_SET_STATE);
        relationshipSet.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(relationshipSet);

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(VALUE_TO_TRACK);
        properties.add(TIME_WINDOW);
        properties.add(SUB_WINDOW_LENGTH);
        this.properties = Collections.unmodifiableList(properties);
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
    public void onScheduled(final ProcessContext context) throws IOException {
        timeWindow = context.getProperty(TIME_WINDOW).asTimePeriod(TimeUnit.MILLISECONDS);
        microBatchTime = context.getProperty(SUB_WINDOW_LENGTH).asTimePeriod(TimeUnit.MILLISECONDS);

        if(microBatchTime == null || microBatchTime == 0) {
            StateManager stateManager = context.getStateManager();
            StateMap state = stateManager.getState(SCOPE);
            HashMap<String, String> tempMap = new HashMap<>();
            tempMap.putAll(state.toMap());
            if (!tempMap.containsKey(COUNT_KEY)) {
                tempMap.put(COUNT_KEY, "0");
                context.getStateManager().setState(tempMap, SCOPE);
            }
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try {
            Long currTime = System.currentTimeMillis();
            if(microBatchTime == null){
                noMicroBatch(context, session, flowFile, currTime);
            } else{
                microBatch(context, session, flowFile, currTime);
            }

        } catch (Exception e) {
            getLogger().error("Ran into an error while processing {}.", flowFile, e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private void noMicroBatch(ProcessContext context, ProcessSession session, FlowFile flowFile, Long currTime) {
        Map<String, String> state = null;
        try {
            state = new HashMap<>(session.getState(SCOPE).toMap());
        } catch (IOException e) {
            getLogger().error("Failed to get the initial state when processing {}; transferring FlowFile back to its incoming queue", flowFile, e);
            session.transfer(flowFile);
            context.yield();
            return;
        }

        Long count = Long.valueOf(state.get(COUNT_KEY));
        count ++;

        Set<String> keysToRemove = new HashSet<>();

        for(String key: state.keySet()){
            if(!key.equals(COUNT_KEY)){
                Long timeStamp = Long.decode(key);

                if(currTime - timeStamp > timeWindow) {
                    keysToRemove.add(key);
                    count --;

                }
            }
        }
        String countString = String.valueOf(count);

        for(String key: keysToRemove){
            state.remove(key);
        }

        Double aggregateValue = 0.0D;
        Variance variance = new Variance();

        for(Map.Entry<String,String> entry: state.entrySet()){
            if(!entry.getKey().equals(COUNT_KEY)){
                final Double value = Double.valueOf(entry.getValue());
                variance.increment(value);
                aggregateValue += value ;
            }
        }

        final Double currentFlowFileValue = context.getProperty(VALUE_TO_TRACK).evaluateAttributeExpressions(flowFile).asDouble();
        variance.increment(currentFlowFileValue);
        aggregateValue += currentFlowFileValue;

        state.put(String.valueOf(currTime), String.valueOf(currentFlowFileValue));
        state.put(COUNT_KEY, countString);

        try {
            session.setState(state, SCOPE);
        } catch (IOException e) {
            getLogger().error("Failed to set the state after successfully processing {} due a failure when setting the state. Transferring to '{}'",
                    flowFile, REL_FAILED_SET_STATE.getName(), e);

            session.transfer(flowFile, REL_FAILED_SET_STATE);
            context.yield();
            return;
        }

        Double mean = aggregateValue / count;

        Map<String, String> attributesToAdd = new HashMap<>();
        attributesToAdd.put(ROLLING_WINDOW_VALUE_KEY, String.valueOf(aggregateValue));
        attributesToAdd.put(ROLLING_WINDOW_COUNT_KEY, String.valueOf(count));
        attributesToAdd.put(ROLLING_WINDOW_MEAN_KEY, String.valueOf(mean));
        double varianceValue = variance.getResult();
        attributesToAdd.put(ROLLING_WINDOW_VARIANCE_KEY, String.valueOf(varianceValue));
        attributesToAdd.put(ROLLING_WINDOW_STDDEV_KEY, String.valueOf(Math.sqrt(varianceValue)));

        flowFile = session.putAllAttributes(flowFile, attributesToAdd);

        session.transfer(flowFile, REL_SUCCESS);
    }

    private void microBatch(ProcessContext context, ProcessSession session, FlowFile flowFile, Long currTime) {
        Map<String, String> state = null;
        try {
            state = new HashMap<>(session.getState(SCOPE).toMap());
        } catch (IOException e) {
            getLogger().error("Failed to get the initial state when processing {}; transferring FlowFile back to its incoming queue", flowFile, e);
            session.transfer(flowFile);
            context.yield();
            return;
        }

        String currBatchStart = state.get(CURRENT_MICRO_BATCH_STATE_TS_KEY);
        boolean newBatch = false;
        if(currBatchStart != null){
            if (currTime - Long.valueOf(currBatchStart) > microBatchTime) {
                newBatch = true;
                currBatchStart = String.valueOf(currTime);
                state.put(CURRENT_MICRO_BATCH_STATE_TS_KEY, currBatchStart);
            }
        } else {
            newBatch = true;
            currBatchStart = String.valueOf(currTime);
            state.put(CURRENT_MICRO_BATCH_STATE_TS_KEY, currBatchStart);
        }

        Long count = 0L;
        count += 1;

        Set<String> keysToRemove = new HashSet<>();

        for(String key: state.keySet()){
            String timeStampString;
            if (key.endsWith(BATCH_APPEND_KEY)) {
                timeStampString = key.substring(0, key.length() - COUNT_APPEND_KEY_LENGTH);
                Long timeStamp = Long.decode(timeStampString);

                if (currTime - timeStamp  > timeWindow) {
                    keysToRemove.add(key);
                }
            } else if(key.endsWith(COUNT_APPEND_KEY)) {
                timeStampString = key.substring(0, key.length() - COUNT_APPEND_KEY_LENGTH);
                Long timeStamp = Long.decode(timeStampString);

                if (currTime - timeStamp > timeWindow) {
                    keysToRemove.add(key);
                } else {
                    count += Long.valueOf(state.get(key));
                }
            }
        }

        for(String key:keysToRemove){
            state.remove(key);
        }
        keysToRemove.clear();

        Double aggregateValue = 0.0D;
        Double currentBatchValue =  0.0D;
        Long currentBatchCount = 0L;
        Variance variance = new Variance();

        for(Map.Entry<String,String> entry: state.entrySet()){
            String key = entry.getKey();
            if (key.endsWith(BATCH_APPEND_KEY)) {
                String timeStampString = key.substring(0, key.length() - COUNT_APPEND_KEY_LENGTH);

                Double batchValue = Double.valueOf(entry.getValue());
                Long batchCount = Long.valueOf(state.get(timeStampString + COUNT_APPEND_KEY));
                if (!newBatch && timeStampString.equals(currBatchStart)) {

                    final Double currentFlowFileValue = context.getProperty(VALUE_TO_TRACK).evaluateAttributeExpressions(flowFile).asDouble();
                    batchCount++;

                    batchValue += currentFlowFileValue;
                    currentBatchValue = batchValue;
                    currentBatchCount = batchCount;
                }

                aggregateValue += batchValue;
                variance.increment(batchValue);
            }
        }

        if (newBatch) {
            final Double currentFlowFileValue = context.getProperty(VALUE_TO_TRACK).evaluateAttributeExpressions(flowFile).asDouble();

            currentBatchValue += currentFlowFileValue;
            currentBatchCount = 1L;

            aggregateValue += currentBatchValue;
            variance.increment(currentBatchValue);
        }

        state.put(currBatchStart + BATCH_APPEND_KEY, String.valueOf(currentBatchValue));
        state.put(currBatchStart + COUNT_APPEND_KEY, String.valueOf(currentBatchCount));

        try {
            session.setState(state, SCOPE);
        } catch (IOException e) {
            getLogger().error("Failed to get the initial state when processing {}; transferring FlowFile back to its incoming queue", flowFile, e);
            session.transfer(flowFile);
            context.yield();
            return;
        }

        Double mean = aggregateValue / count;

        Map<String, String> attributesToAdd = new HashMap<>();
        attributesToAdd.put(ROLLING_WINDOW_VALUE_KEY, String.valueOf(aggregateValue));
        attributesToAdd.put(ROLLING_WINDOW_COUNT_KEY, String.valueOf(count));
        attributesToAdd.put(ROLLING_WINDOW_MEAN_KEY, String.valueOf(mean));
        double varianceValue = variance.getResult();
        attributesToAdd.put(ROLLING_WINDOW_VARIANCE_KEY, String.valueOf(varianceValue));
        attributesToAdd.put(ROLLING_WINDOW_STDDEV_KEY, String.valueOf(Math.sqrt(varianceValue)));

        flowFile = session.putAllAttributes(flowFile, attributesToAdd);

        session.transfer(flowFile, REL_SUCCESS);
    }
}
