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
package org.apache.nifi.websocket.jetty;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.websocket.WebSocketConfigurationException;
import org.apache.nifi.websocket.WebSocketMessageRouter;
import org.apache.nifi.websocket.WebSocketServerService;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.DefaultAuthenticatorFactory;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.util.security.Constraint;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketPolicy;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.eclipse.jetty.websocket.servlet.WebSocketCreator;
import org.eclipse.jetty.websocket.servlet.WebSocketServlet;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Tags({"WebSocket", "Jetty", "server"})
@CapabilityDescription("Triển khai WebSocketServerService." +
        " Dịch vụ này sử dụng module Jetty WebSocket server để cung cấp" +
        " quản lý phiên WebSocket trong toàn ứng dụng.")
public class JettyWebSocketServer extends AbstractJettyWebSocketService implements WebSocketServerService {

    /**
     * Bản đồ toàn cục để tham chiếu một instance controller service theo số cổng yêu cầu.
     */
    private static final Map<Integer, JettyWebSocketServer> portToControllerService = new ConcurrentHashMap<>();

    // Các giá trị cho xác thực client
    public static final AllowableValue CLIENT_NONE = new AllowableValue("no", "Không xác thực",
            "Processor sẽ không xác thực client. Bất kỳ ai cũng có thể giao tiếp với Processor một cách ẩn danh");
    public static final AllowableValue CLIENT_WANT = new AllowableValue("want", "Muốn xác thực",
            "Processor sẽ cố gắng xác thực client nhưng nếu không xác thực được sẽ cho phép client giao tiếp ẩn danh");
    public static final AllowableValue CLIENT_NEED = new AllowableValue("need", "Cần xác thực",
            "Processor sẽ từ chối giao tiếp từ bất kỳ client nào trừ khi client cung cấp chứng chỉ được TrustStore trong SSL Context Service chấp nhận");

    public static final AllowableValue LOGIN_SERVICE_HASH = new AllowableValue("hash", "HashLoginService",
            "Chi tiết xem tại http://www.eclipse.org/jetty/javadoc/current/org/eclipse/jetty/security/HashLoginService.html");

    public static final PropertyDescriptor CLIENT_AUTH = new PropertyDescriptor.Builder()
            .name("client-authentication")
            .displayName("Xác thực SSL Client")
            .description("Chỉ định Processor có xác thực client bằng chứng chỉ hay không. "
                    + "Giá trị này bị bỏ qua nếu <SSL Context Service> không được cấu hình hoặc SSL Context chỉ dùng KeyStore mà không có TrustStore.")
            .required(true)
            .allowableValues(CLIENT_NONE, CLIENT_WANT, CLIENT_NEED)
            .defaultValue(CLIENT_NONE.getValue())
            .build();

    public static final PropertyDescriptor LISTEN_PORT = new PropertyDescriptor.Builder()
            .name("listen-port")
            .displayName("Cổng lắng nghe")
            .description("Số cổng mà WebSocketServer sẽ lắng nghe.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    public static final PropertyDescriptor BASIC_AUTH = new PropertyDescriptor.Builder()
            .name("basic-auth")
            .displayName("Bật Basic Authentication")
            .description("Nếu bật, các yêu cầu kết nối từ client sẽ được xác thực bằng Basic Authentication với Login Provider chỉ định.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor AUTH_PATH_SPEC = new PropertyDescriptor.Builder()
            .name("auth-path-spec")
            .displayName("Path Spec cho Basic Authentication")
            .description("Chỉ định Path Spec để áp dụng Basic Authentication.")
            .required(false)
            .defaultValue("/*")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor AUTH_ROLES = new PropertyDescriptor.Builder()
            .name("auth-roles")
            .displayName("Roles cho Basic Authentication")
            .description("Người dùng xác thực phải có ít nhất một trong các role chỉ định. "
                    + "Nhiều role có thể được đặt bằng chuỗi phân tách bằng dấu phẩy. "
                    + "'*' hoặc '**' đại diện cho bất kỳ role nào kể cả không có role.")
            .required(false)
            .defaultValue("**")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor LOGIN_SERVICE = new PropertyDescriptor.Builder()
            .name("login-service")
            .displayName("Login Service")
            .description("Chỉ định Login Service nào được sử dụng cho Basic Authentication.")
            .required(false)
            .allowableValues(LOGIN_SERVICE_HASH)
            .defaultValue(LOGIN_SERVICE_HASH.getValue())
            .build();

    public static final PropertyDescriptor USERS_PROPERTIES_FILE = new PropertyDescriptor.Builder()
            .name("users-properties-file")
            .displayName("File Properties của Users")
            .description("Chỉ định file properties chứa thông tin người dùng cho Basic Authentication với HashLoginService. "
                    + "Chi tiết xem tại http://www.eclipse.org/jetty/documentation/current/configuring-security.html")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
            .build();


    private static final List<PropertyDescriptor> properties;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.addAll(getAbstractPropertyDescriptors());
        props.add(LISTEN_PORT);
        props.add(SSL_CONTEXT);
        props.add(CLIENT_AUTH);
        props.add(BASIC_AUTH);
        props.add(AUTH_PATH_SPEC);
        props.add(AUTH_ROLES);
        props.add(LOGIN_SERVICE);
        props.add(USERS_PROPERTIES_FILE);


        properties = Collections.unmodifiableList(props);
    }

    private WebSocketPolicy configuredPolicy;
    private Server server;
    private Integer listenPort;
    private ServletHandler servletHandler;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }


    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {

        final List<ValidationResult> results = new ArrayList<>();
        if (validationContext.getProperty(BASIC_AUTH).asBoolean()) {
            final String loginServiceValue = validationContext.getProperty(LOGIN_SERVICE).getValue();
            if (LOGIN_SERVICE_HASH.equals(loginServiceValue)) {
                if (!validationContext.getProperty(USERS_PROPERTIES_FILE).isSet()) {
                    results.add(new ValidationResult.Builder().subject(USERS_PROPERTIES_FILE.getDisplayName())
                            .explanation("it is required by HashLoginService").valid(false).build());
                }
            }
        }

        return results;
    }

    public static class JettyWebSocketServlet extends WebSocketServlet implements WebSocketCreator {
        @Override
        public void configure(WebSocketServletFactory webSocketServletFactory) {
            webSocketServletFactory.setCreator(this);
        }

        @Override
        public Object createWebSocket(ServletUpgradeRequest servletUpgradeRequest, ServletUpgradeResponse servletUpgradeResponse) {
            final URI requestURI = servletUpgradeRequest.getRequestURI();
            final int port = servletUpgradeRequest.getLocalPort();
            final JettyWebSocketServer service = portToControllerService.get(port);

            if (service == null) {
                throw new RuntimeException("No controller service is bound with port: " + port);
            }

            final String path = requestURI.getPath();
            final WebSocketMessageRouter router;
            try {
                router = service.routers.getRouterOrFail(path);
            } catch (WebSocketConfigurationException e) {
                throw new IllegalStateException("Failed to get router due to: "  + e, e);
            }

            final RoutingWebSocketListener listener = new RoutingWebSocketListener(router) {
                @Override
                public void onWebSocketConnect(Session session) {
                    final WebSocketPolicy currentPolicy = session.getPolicy();
                    currentPolicy.setInputBufferSize(service.configuredPolicy.getInputBufferSize());
                    currentPolicy.setMaxTextMessageSize(service.configuredPolicy.getMaxTextMessageSize());
                    currentPolicy.setMaxBinaryMessageSize(service.configuredPolicy.getMaxBinaryMessageSize());
                    super.onWebSocketConnect(session);
                }
            };

            return listener;
        }
    }

    @OnEnabled
    @Override
    public void startServer(final ConfigurationContext context) throws Exception {

        if (server != null && server.isRunning()) {
            getLogger().info("A WebSocket server is already running. {}", server);
            return;
        }

        configuredPolicy = WebSocketPolicy.newServerPolicy();
        configurePolicy(context, configuredPolicy);

        server = new Server();

        final ContextHandlerCollection handlerCollection = new ContextHandlerCollection();

        final ServletContextHandler contextHandler = new ServletContextHandler();
        // Set ClassLoader so that jetty-server classes are available to WebSocketServletFactory.Loader
        contextHandler.setClassLoader(getClass().getClassLoader());

        // Add basic auth.
        if (context.getProperty(BASIC_AUTH).asBoolean()) {
            final ConstraintSecurityHandler securityHandler = new ConstraintSecurityHandler();
            contextHandler.insertHandler(securityHandler);

            final Constraint constraint = new Constraint();
            constraint.setName("auth");
            constraint.setAuthenticate(true);
            // Accessible from any role and any auth once authentication succeeds.
            final String roles = context.getProperty(AUTH_ROLES).evaluateAttributeExpressions().getValue();
            constraint.setRoles(roles.split(","));

            final ConstraintMapping constraintMapping = new ConstraintMapping();
            constraintMapping.setPathSpec(context.getProperty(AUTH_PATH_SPEC).evaluateAttributeExpressions().getValue());
            constraintMapping.setConstraint(constraint);

            final DefaultAuthenticatorFactory authenticatorFactory = new DefaultAuthenticatorFactory();
            securityHandler.setAuthenticatorFactory(authenticatorFactory);
            securityHandler.setAuthMethod(Constraint.__BASIC_AUTH);
            securityHandler.setRealmName(getClass().getSimpleName());
            securityHandler.setConstraintMappings(Collections.singletonList(constraintMapping));

            final LoginService loginService;
            final String loginServiceValue = context.getProperty(LOGIN_SERVICE).getValue();
            if (LOGIN_SERVICE_HASH.equals(loginServiceValue)) {
                final String usersFilePath = context.getProperty(USERS_PROPERTIES_FILE).evaluateAttributeExpressions().getValue();
                loginService = new HashLoginService("HashLoginService", usersFilePath);
            } else {
                throw new IllegalArgumentException("Unsupported Login Service: " + loginServiceValue);
            }

            server.addBean(loginService);
            securityHandler.setLoginService(loginService);
        }

        servletHandler = new ServletHandler();
        contextHandler.insertHandler(servletHandler);

        handlerCollection.setHandlers(new Handler[]{contextHandler});

        server.setHandler(handlerCollection);


        listenPort = context.getProperty(LISTEN_PORT).evaluateAttributeExpressions().asInteger();
        final SslContextFactory sslContextFactory = createSslFactory(context);

        final ServerConnector serverConnector = createConnector(sslContextFactory, listenPort);
        server.setConnectors(new Connector[] {serverConnector});

        servletHandler.addServletWithMapping(JettyWebSocketServlet.class, "/*");

        getLogger().info("Starting JettyWebSocketServer on port {}.", listenPort);
        server.start();
        listenPort = serverConnector.getLocalPort();

        portToControllerService.put(listenPort, this);
    }

    public int getListeningPort() {
        return listenPort;
    }

    private ServerConnector createConnector(final SslContextFactory sslContextFactory, final Integer listenPort) {

        final ServerConnector serverConnector;
        if (sslContextFactory == null) {
            serverConnector = new ServerConnector(server);
        } else {
            final HttpConfiguration httpsConfiguration = new HttpConfiguration();
            httpsConfiguration.setSecureScheme("https");
            httpsConfiguration.addCustomizer(new SecureRequestCustomizer());
            serverConnector = new ServerConnector(server,
                    new SslConnectionFactory(sslContextFactory, "http/1.1"),
                    new HttpConnectionFactory(httpsConfiguration));
        }
        serverConnector.setPort(listenPort);
        return serverConnector;
    }

    private SslContextFactory createSslFactory(final ConfigurationContext context) {
        final SSLContextService sslService = context.getProperty(SSL_CONTEXT).asControllerService(SSLContextService.class);

        final String clientAuthValue = context.getProperty(CLIENT_AUTH).getValue();
        final boolean need;
        final boolean want;
        if (CLIENT_NEED.equals(clientAuthValue)) {
            need = true;
            want = false;
        } else if (CLIENT_WANT.equals(clientAuthValue)) {
            need = false;
            want = true;
        } else {
            need = false;
            want = false;
        }

        final SslContextFactory sslFactory = (sslService == null) ? null : createSslFactory(sslService, need, want, null);
        return sslFactory;
    }

    @OnDisabled
    @OnShutdown
    @Override
    public void stopServer() throws Exception {
        if (server == null) {
            return;
        }

        getLogger().info("Stopping JettyWebSocketServer.");
        server.stop();
        if (portToControllerService.containsKey(listenPort)
                && this.getIdentifier().equals(portToControllerService.get(listenPort).getIdentifier())) {
            portToControllerService.remove(listenPort);
        }
    }

}
