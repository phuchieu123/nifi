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
package org.apache.nifi.lookup;

import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.lookup.configuration2.CommonsConfigurationLookupService;
import org.apache.nifi.lookup.configuration2.SafeXMLConfiguration;


@Tags({"lookup", "cache", "enrich", "join", "xml", "reloadable", "key", "value"})
@CapabilityDescription("Một dịch vụ tra cứu dựa trên tệp XML có thể tải lại." +
        " Dịch vụ này sử dụng Apache Commons Configuration." +
        " Ví dụ về tệp cấu hình XML và cách truy cập cấu hình cụ thể có thể được tìm thấy tại" +
        " http://commons.apache.org/proper/commons-configuration/userguide/howto_hierarchical.html." +
        " Việc xử lý thực thể bên ngoài bị vô hiệu hóa.")
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.READ_FILESYSTEM,
                        explanation = "Provides operator the ability to read from any file that NiFi has access to.")
        }
)
public class XMLFileLookupService extends CommonsConfigurationLookupService<SafeXMLConfiguration> {

}
