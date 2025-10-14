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
package org.apache.nifi.processors.azure.storage.utils;

import org.apache.nifi.components.DescribedValue;

public enum WritingStrategy implements DescribedValue {

    WRITE_AND_RENAME("Write and Rename", 
        "Processor ghi tệp Azure vào thư mục tạm, sau đó đổi tên/chuyển nó đến vị trí đích cuối cùng. " +
        "Cách này giúp ngăn chặn các tiến trình khác đọc phải tệp chưa được ghi hoàn tất."),
        
    SIMPLE_WRITE("Simple Write", 
        "Processor ghi tệp Azure trực tiếp đến vị trí đích. " +
        "Cách này có thể khiến tiến trình khác đọc phải tệp chưa được ghi xong.");

    private final String displayName;
    private final String description;

    WritingStrategy(String displayName, String description) {
        this.displayName = displayName;
        this.description = description;
    }

    @Override
    public String getValue() {
        return name();
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
