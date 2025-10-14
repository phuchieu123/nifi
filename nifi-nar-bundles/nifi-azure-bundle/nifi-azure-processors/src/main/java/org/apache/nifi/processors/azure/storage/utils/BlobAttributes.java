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
// 📦 Thuộc tính dùng cho Azure Blob trong NiFi
public final class BlobAttributes {

    // 🪣 Tên container chứa blob
    public static final String ATTR_NAME_CONTAINER = "azure.container";
    public static final String ATTR_DESCRIPTION_CONTAINER = "Tên của container trên Azure Blob Storage";

    // 📄 Tên blob (tệp trên Azure)
    public static final String ATTR_NAME_BLOBNAME = "azure.blobname";
    public static final String ATTR_DESCRIPTION_BLOBNAME = "Tên của blob trên Azure Blob Storage";

    // 🌐 Đường dẫn chính đến blob
    public static final String ATTR_NAME_PRIMARY_URI = "azure.primaryUri";
    public static final String ATTR_DESCRIPTION_PRIMARY_URI = "Vị trí chính (URI) của blob";

    // 🧾 ETag của blob (dùng để xác thực phiên bản)
    public static final String ATTR_NAME_ETAG = "azure.etag";
    public static final String ATTR_DESCRIPTION_ETAG = "ETag (mã nhận dạng phiên bản) của blob";

    // 🧱 Loại blob (Block, Page hoặc Append)
    public static final String ATTR_NAME_BLOBTYPE = "azure.blobtype";
    public static final String ATTR_DESCRIPTION_BLOBTYPE = "Loại của blob (BlockBlob, PageBlob hoặc AppendBlob)";

    // 📑 Kiểu MIME của nội dung
    public static final String ATTR_NAME_MIME_TYPE = "mime.type";
    public static final String ATTR_DESCRIPTION_MIME_TYPE = "Kiểu MIME của nội dung";

    // 🌍 Mã ngôn ngữ của nội dung
    public static final String ATTR_NAME_LANG = "lang";
    public static final String ATTR_DESCRIPTION_LANG = "Mã ngôn ngữ của nội dung";

    // ⏱️ Dấu thời gian của blob
    public static final String ATTR_NAME_TIMESTAMP = "azure.timestamp";
    public static final String ATTR_DESCRIPTION_TIMESTAMP = "Dấu thời gian (timestamp) của blob";

    // 📏 Độ dài (kích thước) của blob
    public static final String ATTR_NAME_LENGTH = "azure.length";
    public static final String ATTR_DESCRIPTION_LENGTH = "Độ dài hoặc kích thước của blob";

    // ⚠️ Mã lỗi khi thao tác blob thất bại
    public static final String ATTR_NAME_ERROR_CODE = "azure.error.code";
    public static final String ATTR_DESCRIPTION_ERROR_CODE = "Mã lỗi được báo cáo trong quá trình thao tác blob";

    // 🚫 Trạng thái bị bỏ qua (khi chọn chiến lược 'ignore')
    public static final String ATTR_NAME_IGNORED = "azure.ignored";
    public static final String ATTR_DESCRIPTION_IGNORED = 
        "Khi chiến lược xử lý xung đột là 'ignore', thuộc tính này sẽ là true/false tùy theo blob có bị bỏ qua hay không.";
}
