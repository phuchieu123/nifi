/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
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

package org.apache.nifi.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.StringLookupService;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
@CapabilityDescription("Tìm kiếm một giá trị chuỗi từ Elasticsearch Server liên kết với ID tài liệu được chỉ định. " +
        "Các tọa độ được truyền đến tìm kiếm phải chứa khóa 'id'.")
@Tags({"lookup", "enrich", "value", "key", "elasticsearch"})
public class ElasticSearchStringLookupService extends AbstractControllerService implements StringLookupService {
    public static final PropertyDescriptor CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("el-rest-client-service")
            .displayName("Dịch vụ Máy khách")
            .description("Một dịch vụ máy khách ElasticSearch sẽ được sử dụng để chạy các truy vấn.")
            .identifiesControllerService(ElasticSearchClientService.class)
            .required(true)
            .build();
    public static final PropertyDescriptor INDEX = new PropertyDescriptor.Builder()
            .name("el-lookup-index")
            .displayName("Chỉ mục")
            .description("Tên của chỉ mục để đọc từ")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor TYPE = new PropertyDescriptor.Builder()
            .name("el-lookup-type")
            .displayName("Loại")
            .description("Loại của tài liệu này (được Elasticsearch sử dụng để lập chỉ mục và tìm kiếm)")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    private static final List<PropertyDescriptor> DESCRIPTORS = Arrays.asList(CLIENT_SERVICE, INDEX, TYPE);
    private static final ObjectMapper mapper = new ObjectMapper();
    public static final String ID = "es_document_id";
    private ElasticSearchClientService esClient;
    private String index;
    private String type;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        esClient = context.getProperty(CLIENT_SERVICE).asControllerService(ElasticSearchClientService.class);
        index = context.getProperty(INDEX).evaluateAttributeExpressions().getValue();
        type = context.getProperty(TYPE).evaluateAttributeExpressions().getValue();
    }

    @Override
    public Optional<String> lookup(final Map<String, Object> coordinates) throws LookupFailureException {
        try {
            final String id = (String) coordinates.get(ID);
            final Map<String, Object> enums = esClient.get(index, type, id, null);
            if (enums == null) {
                return Optional.empty();
            } else {
                return Optional.ofNullable(mapper.writeValueAsString(enums));
            }
        } catch (final IOException | ElasticsearchException e) {
            throw new LookupFailureException(e);
        }
    }

    @Override
    public Set<String> getRequiredKeys() {
        return Collections.singleton(ID);
    }
}
