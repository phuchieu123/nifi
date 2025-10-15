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
package org.apache.nifi.db.schemaregistry;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Tags({"schema", "registry", "database", "table"})
@CapabilityDescription("Cung cấp một dịch vụ để tạo lược đồ bản ghi từ định nghĩa bảng cơ sở dữ liệu. Dịch vụ được cấu hình "
        + "để sử dụng tên bảng và kết nối cơ sở dữ liệu để tìm nạp siêu dữ liệu của bảng (tức là định nghĩa bảng) chẳng hạn như tên cột, kiểu dữ liệu, "
        + "khả năng null, v.v.")
public class DatabaseTableSchemaRegistry extends AbstractControllerService implements SchemaRegistry {

    private static final Set<SchemaField> schemaFields = EnumSet.of(SchemaField.SCHEMA_NAME);

    static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("Database Connection Pooling Service")
            .displayName("Dịch vụ Gộp kết nối Cơ sở dữ liệu")
            .description("Dịch vụ Điều khiển (Controller Service) được sử dụng để lấy kết nối đến cơ sở dữ liệu nhằm truy xuất thông tin bảng.")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    static final PropertyDescriptor CATALOG_NAME = new PropertyDescriptor.Builder()
            .name("Catalog Name")
            .displayName("Tên Catalog")
            .description("Tên của catalog được sử dụng để xác định vị trí bảng mong muốn. Điều này có thể không áp dụng cho cơ sở dữ liệu bạn đang truy vấn. Trong trường hợp này, hãy để trống trường. Lưu ý rằng nếu "
                    + "thuộc tính được đặt và cơ sở dữ liệu phân biệt chữ hoa chữ thường, tên catalog phải khớp chính xác với tên catalog của cơ sở dữ liệu.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor SCHEMA_NAME = new PropertyDescriptor.Builder()
            .name("Schema Name")
            .displayName("Tên Lược đồ")
            .description("Tên của lược đồ mà bảng thuộc về. Điều này có thể không áp dụng cho cơ sở dữ liệu bạn đang cập nhật. Trong trường hợp này, hãy để trống trường. Lưu ý rằng nếu "
                    + "thuộc tính được đặt và cơ sở dữ liệu phân biệt chữ hoa chữ thường, tên lược đồ phải khớp chính xác với tên lược đồ của cơ sở dữ liệu. Cũng lưu ý rằng nếu cùng một tên bảng tồn tại trong nhiều "
                    + "lược đồ và Tên lược đồ không được chỉ định, dịch vụ sẽ tìm thấy các bảng đó và báo lỗi nếu các bảng khác nhau có cùng (các) tên cột.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected List<PropertyDescriptor> propDescriptors = Collections.unmodifiableList(Arrays.asList(
            DBCP_SERVICE,
            CATALOG_NAME,
            SCHEMA_NAME
    ));

    private volatile DBCPService dbcpService;
    private volatile String dbCatalogName;
    private volatile String dbSchemaName;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propDescriptors;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
        dbCatalogName = context.getProperty(CATALOG_NAME).evaluateAttributeExpressions().getValue();
        dbSchemaName = context.getProperty(SCHEMA_NAME).evaluateAttributeExpressions().getValue();
    }

    @Override
    public RecordSchema retrieveSchema(SchemaIdentifier schemaIdentifier) throws IOException, SchemaNotFoundException {
        if (schemaIdentifier.getName().isPresent()) {
            return retrieveSchemaByName(schemaIdentifier);
        } else {
            throw new SchemaNotFoundException("This Schema Registry only supports retrieving a schema by name.");
        }
    }

    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return schemaFields;
    }

    RecordSchema retrieveSchemaByName(final SchemaIdentifier schemaIdentifier) throws IOException, SchemaNotFoundException {
        final Optional<String> schemaName = schemaIdentifier.getName();
        if (!schemaName.isPresent()) {
            throw new SchemaNotFoundException("Cannot retrieve schema because Schema Name is not present");
        }

        final String tableName = schemaName.get();
        try {
            try (final Connection conn = dbcpService.getConnection()) {
                final DatabaseMetaData databaseMetaData = conn.getMetaData();
                    return getRecordSchemaFromMetadata(databaseMetaData, tableName);
                }
        } catch (SQLException sqle) {
            throw new IOException("Error retrieving schema for table " + schemaName.get(), sqle);
        }
    }

    private RecordSchema getRecordSchemaFromMetadata(final DatabaseMetaData databaseMetaData, final String tableName) throws SQLException, SchemaNotFoundException {
        try (final ResultSet columnResultSet = databaseMetaData.getColumns(dbCatalogName, dbSchemaName, tableName, "%")) {

            final List<RecordField> recordFields = new ArrayList<>();
            while (columnResultSet.next()) {
                recordFields.add(createRecordFieldFromColumn(columnResultSet));
            }

            // If no columns are found, check that the table exists
            if (recordFields.isEmpty()) {
                checkTableExists(databaseMetaData, tableName);
            }
            return new SimpleRecordSchema(recordFields);
        }
    }

    private RecordField createRecordFieldFromColumn(final ResultSet columnResultSet) throws SQLException {
        // COLUMN_DEF must be read first to work around Oracle bug, see NIFI-4279 for details
        final String defaultValue = columnResultSet.getString("COLUMN_DEF");
        final String columnName = columnResultSet.getString("COLUMN_NAME");
        String typeName = columnResultSet.getString("TYPE_NAME");
        final int dataType;
        if (typeName.equalsIgnoreCase("bool")) {
            dataType = 16;
        } else {
            dataType = columnResultSet.getInt("DATA_TYPE");
        }
        final String nullableValue = columnResultSet.getString("IS_NULLABLE");
        final boolean isNullable = "YES".equalsIgnoreCase(nullableValue) || nullableValue.isEmpty();
        return new RecordField(
                columnName,
                DataTypeUtils.getDataTypeFromSQLTypeValue(dataType),
                defaultValue,
                isNullable);
    }

    private void checkTableExists(final DatabaseMetaData databaseMetaData, final String tableName) throws SchemaNotFoundException, SQLException {
        try (final ResultSet tablesResultSet = databaseMetaData.getTables(dbCatalogName, dbSchemaName, tableName, null)) {
            final List<String> qualifiedNameSegments = new ArrayList<>();
            if (dbCatalogName != null) {
                qualifiedNameSegments.add(dbCatalogName);
            }
            if (dbSchemaName != null) {
                qualifiedNameSegments.add(dbSchemaName);
            }
            qualifiedNameSegments.add(tableName);

            final String qualifiedTableName = String.join(".", qualifiedNameSegments);
            if (tablesResultSet.next()) {
                getLogger().warn("No columns found for Table [{}] check permissions for retrieving schema definitions", qualifiedTableName);
            } else {
                throw new SchemaNotFoundException(String.format("Table [%s] not found", qualifiedTableName));
            }
        }
    }
}