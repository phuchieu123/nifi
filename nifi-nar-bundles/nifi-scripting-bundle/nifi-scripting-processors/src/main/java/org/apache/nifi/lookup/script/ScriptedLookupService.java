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
package org.apache.nifi.lookup.script;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.LookupService;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * A Controller service that allows the user to script the lookup operation to be performed (by LookupRecord, e.g.)
 */
@Tags({"lookup", "record", "script", "invoke", "groovy", "python", "jython", "jruby", "ruby", "javascript", "js", "lua", "luaj"})
@CapabilityDescription("Cho phép người dùng cung cấp một instance LookupService được viết bằng script để làm giàu các bản ghi từ flow file đến. " +
        "Lưu ý rằng, do một lỗi trong Jython chưa được giải quyết, không thể sử dụng Jython để viết script cho dịch vụ này bằng Python.")
@DynamicProperty(
        name = "Thuộc tính binding của Script Engine",
        value = "Giá trị thuộc tính binding được truyền vào Script Runner",
        expressionLanguageScope = ExpressionLanguageScope.VARIABLE_REGISTRY,
        description = "Cập nhật một thuộc tính của script engine được chỉ định bởi key của Dynamic Property với giá trị được chỉ định bởi value của Dynamic Property"
)
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.EXECUTE_CODE,
                        explanation = "Cho phép người điều hành thực thi mã tùy ý với tất cả quyền mà NiFi có."
                )
        }
)

public class ScriptedLookupService extends BaseScriptedLookupService implements LookupService<Object> {

    @Override
    @SuppressWarnings("unchecked")
    public Set<String> getRequiredKeys() {
        return lookupService.get().getRequiredKeys();
    }

    @Override
    public Class<?> getValueType() {
        // Delegate the getValueType() call to the scripted LookupService
        return lookupService.get().getValueType();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Optional<Object> lookup(Map<String, Object> coordinates) throws LookupFailureException {
        // Delegate the lookup() call to the scripted LookupService
        return lookupService.get().lookup(coordinates);
    }
}
