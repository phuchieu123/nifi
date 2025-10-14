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
package org.apache.nifi.jms.cf;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;

import javax.jms.ConnectionFactory;
import java.util.List;

@Tags({"jms", "jndi", "messaging", "integration", "queue", "topic", "publish", "subscribe"})
@CapabilityDescription("Cung cấp dịch vụ để tra cứu một JMS ConnectionFactory hiện có sử dụng Java Naming and Directory Interface (JNDI).")
@DynamicProperty(
    description = "Để thực hiện JNDI Lookup, một Initial Context phải được thiết lập. Khi đó, một Environment có thể được cấu hình cho context này. "
        + "Bất kỳ thuộc tính động/người dùng nào được thêm vào Controller Service này sẽ được thêm vào như một biến cấu hình Environment cho Context.",
    name = "Tên biến môi trường của JNDI Initial Context.",
    value = "Giá trị của biến môi trường JNDI Initial Context.",
    expressionLanguageScope = ExpressionLanguageScope.VARIABLE_REGISTRY)

@SeeAlso(classNames = {"org.apache.nifi.jms.processors.ConsumeJMS", "org.apache.nifi.jms.processors.PublishJMS", "org.apache.nifi.jms.cf.JMSConnectionFactoryProvider"})
public class JndiJmsConnectionFactoryProvider extends AbstractControllerService implements JMSConnectionFactoryProviderDefinition {

    private volatile JndiJmsConnectionFactoryHandler delegate;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return JndiJmsConnectionFactoryProperties.getPropertyDescriptors();
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return JndiJmsConnectionFactoryProperties.getDynamicPropertyDescriptor(propertyDescriptorName);
    }

    @OnEnabled
    public void onEnabled(ConfigurationContext context) {
        delegate = new JndiJmsConnectionFactoryHandler(context, getLogger());
    }

    @OnDisabled
    public void onDisabled() {
        delegate = null;
    }

    @Override
    public ConnectionFactory getConnectionFactory() {
        return delegate.getConnectionFactory();
    }

    @Override
    public void resetConnectionFactory(ConnectionFactory cachedFactory) {
        delegate.resetConnectionFactory(cachedFactory);
    }
}
