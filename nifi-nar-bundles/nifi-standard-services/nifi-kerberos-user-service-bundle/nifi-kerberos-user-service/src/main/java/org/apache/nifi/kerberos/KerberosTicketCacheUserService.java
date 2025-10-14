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
package org.apache.nifi.kerberos;

import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.security.krb.KerberosTicketCacheUser;
import org.apache.nifi.security.krb.KerberosUser;

import java.util.Collections;
import java.util.List;

@CapabilityDescription("Cung cấp cơ chế tạo KerberosUser từ principal và ticket cache để các component khác "
        + "có thể sử dụng cho việc xác thực Kerberos. Bằng cách đóng gói thông tin này vào Controller Service "
        + "và cho phép các component khác sử dụng, quản trị viên có thể kiểm soát người dùng nào được phép "
        + "sử dụng ticket cache và principal nào. Điều này cung cấp mô hình bảo mật mạnh mẽ hơn cho môi trường đa tenant.")
@Tags({"Kerberos", "Ticket", "Cache", "Principal", "Credentials", "Authentication", "Security"})
@Restricted(restrictions = {
        @Restriction(requiredPermission = RequiredPermission.ACCESS_TICKET_CACHE,
                explanation = "Cho phép người dùng định nghĩa ticket cache và principal, sau đó có thể được các component khác sử dụng.")
})
public class KerberosTicketCacheUserService extends AbstractKerberosUserService implements SelfContainedKerberosUserService {

    static final PropertyDescriptor TICKET_CACHE_FILE = new PropertyDescriptor.Builder()
            .name("Kerberos Ticket Cache File")
            .description("Ticket cache Kerberos liên kết với principal.")
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .build();


    private volatile String ticketCacheFile;

    @Override
    protected List<PropertyDescriptor> getAdditionalProperties() {
        return Collections.singletonList(TICKET_CACHE_FILE);
    }

    @Override
    protected void setAdditionalConfiguredValues(final ConfigurationContext context) {
        this.ticketCacheFile = context.getProperty(TICKET_CACHE_FILE).evaluateAttributeExpressions().getValue();
    }

    @Override
    public KerberosUser createKerberosUser() {
        return new KerberosTicketCacheUser(getPrincipal(), ticketCacheFile);
    }

}
