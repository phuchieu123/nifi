<%--
 Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
--%>
<%@ page contentType="text/html" pageEncoding="UTF-8" session="false" %>
<!DOCTYPE html>
<html lang="en">
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <link rel="stylesheet" href="../nifi/assets/jquery-ui-dist/jquery-ui.min.css" type="text/css" />
        <link rel="stylesheet" href="../nifi/assets/slickgrid/slick.grid.css" type="text/css" />
        <link rel="stylesheet" href="../nifi/css/slick-nifi-theme.css" type="text/css" />
        <link rel="stylesheet" href="../nifi/js/jquery/modal/jquery.modal.css" type="text/css" />
        <link rel="stylesheet" href="../nifi/js/jquery/combo/jquery.combo.css" type="text/css" />
        <link rel="stylesheet" href="../nifi/assets/qtip2/dist/jquery.qtip.min.css" type="text/css" />
        <link rel="stylesheet" href="../nifi/js/codemirror/lib/codemirror.css" type="text/css" />
        <link rel="stylesheet" href="../nifi/js/codemirror/addon/hint/show-hint.css" type="text/css" />
        <link rel="stylesheet" href="../nifi/js/jquery/nfeditor/jquery.nfeditor.css" type="text/css" />
        <link rel="stylesheet" href="../nifi/js/jquery/nfeditor/languages/nfeditor.css" type="text/css" />
        <link rel="stylesheet" href="../nifi/fonts/flowfont/flowfont.css" type="text/css" />
        <link rel="stylesheet" href="../nifi/assets/font-awesome/css/font-awesome.min.css" type="text/css" />
        <link rel="stylesheet" href="../nifi/assets/reset.css/reset.css" type="text/css" />
        <link rel="stylesheet" href="css/main.css" type="text/css" />
        <link rel="stylesheet" href="../nifi/css/common-ui.css" type="text/css" />
        <script type="text/javascript" src="../nifi/assets/jquery/dist/jquery.min.js"></script>
        <script type="text/javascript" src="../nifi/js/jquery/jquery.center.js"></script>
        <script type="text/javascript" src="../nifi/js/jquery/jquery.each.js"></script>
        <script type="text/javascript" src="../nifi/js/jquery/jquery.tab.js"></script>
        <script type="text/javascript" src="../nifi/js/jquery/modal/jquery.modal.js"></script>
        <script type="text/javascript" src="../nifi/js/jquery/combo/jquery.combo.js"></script>
        <script type="text/javascript" src="../nifi/js/jquery/jquery.ellipsis.js"></script>
        <script type="text/javascript" src="../nifi/assets/jquery-ui-dist/jquery-ui.min.js"></script>
        <script type="text/javascript" src="../nifi/assets/qtip2/dist/jquery.qtip.min.js"></script>
        <script type="text/javascript" src="../nifi/assets/JSON2/json2.js"></script>
        <script type="text/javascript" src="../nifi/assets/slickgrid/lib/jquery.event.drag-2.3.0.js"></script>
        <script type="text/javascript" src="../nifi/assets/slickgrid/plugins/slick.cellrangedecorator.js"></script>
        <script type="text/javascript" src="../nifi/assets/slickgrid/plugins/slick.cellrangeselector.js"></script>
        <script type="text/javascript" src="../nifi/assets/slickgrid/plugins/slick.cellselectionmodel.js"></script>
        <script type="text/javascript" src="../nifi/assets/slickgrid/plugins/slick.rowselectionmodel.js"></script>
        <script type="text/javascript" src="../nifi/assets/slickgrid/slick.formatters.js"></script>
        <script type="text/javascript" src="../nifi/assets/slickgrid/slick.editors.js"></script>
        <script type="text/javascript" src="../nifi/assets/slickgrid/slick.dataview.js"></script>
        <script type="text/javascript" src="../nifi/assets/slickgrid/slick.core.js"></script>
        <script type="text/javascript" src="../nifi/assets/slickgrid/slick.grid.js"></script>
        <script type="text/javascript" src="../nifi/js/codemirror/lib/codemirror-compressed.js"></script>
        <script type="text/javascript" src="../nifi/js/nf/nf-namespace.js"></script>
        <script type="text/javascript" src="../nifi/js/nf/nf-authorization-storage.js"></script>
        <script type="text/javascript" src="../nifi/js/nf/nf-storage.js"></script>
        <script type="text/javascript" src="../nifi/js/nf/nf-ajax-setup.js"></script>
        <script type="text/javascript" src="../nifi/js/nf/nf-universal-capture.js"></script>
        <script type="text/javascript" src="../nifi/js/jquery/nfeditor/languages/nfel.js"></script>
        <script type="text/javascript" src="../nifi/js/jquery/nfeditor/jquery.nfeditor.js"></script>
        <script type="text/javascript" src="js/application.js"></script>
       <title>Cập nhật thuộc tính</title>
    </head>
    <body>
        <div id="attribute-updater-processor-id" class="hidden"><%= request.getParameter("id") == null ? "" : org.apache.nifi.util.EscapeUtils.escapeHtml(request.getParameter("id")) %></div>
        <div id="attribute-updater-client-id" class="hidden"><%= request.getParameter("clientId") == null ? "" : org.apache.nifi.util.EscapeUtils.escapeHtml(request.getParameter("clientId")) %></div>
        <div id="attribute-updater-revision" class="hidden"><%= request.getParameter("revision") == null ? "" : org.apache.nifi.util.EscapeUtils.escapeHtml(request.getParameter("revision")) %></div>
        <div id="attribute-updater-editable" class="hidden"><%= request.getParameter("editable") == null ? "" : org.apache.nifi.util.EscapeUtils.escapeHtml(request.getParameter("editable")) %></div>
        <div id="attribute-updater-disconnected-node-acknowledged" class="hidden"><%= request.getParameter("disconnectedNodeAcknowledged") == null ? "false" : org.apache.nifi.util.EscapeUtils.escapeHtml(request.getParameter("disconnectedNodeAcknowledged")) %></div>
        <div id="update-attributes-content">
            <div id="rule-list-panel">
                <div id="flowfile-policy-container">
                    <span id="selected-flowfile-policy" class="hidden"></span>
                    <div id="flowfile-policy-label" class="large-label">Chính sách</div>
                    <div class="info fa fa-question-circle" title="Xác định hành vi khi nhiều quy tắc khớp nhau. Sử dụng clone sẽ đảm bảo rằng mỗi quy tắc khớp nhau được thực thi với một bản sao của flowfile gốc. Sử dụng original sẽ thực thi tất cả các quy tắc khớp nhau với flowfile gốc theo thứ tự được chỉ định bên dưới."></div>
                    <div id="flowfile-policy"></div>
                    <div class="clear"></div>
                </div>
                <div id="rule-label-container">
                    <div id="rules-label" class="large-label">Quy tắc</div>
                    <div class="info fa fa-question-circle" title="Nhấp và kéo để thay đổi thứ tự đánh giá các quy tắc."></div>
                    <button id="new-rule" class="new-rule hidden fa fa-plus"></button>
                    <div class="clear"></div>
                </div>
                <div id="rule-list-container">
                    <ul id="rule-list"></ul>
                </div>
                <div id="no-rules" class="unset">Không tìm thấy quy tắc nào.</div>
                <div id="rule-filter-controls" class="hidden">
                    <div id="rule-filter-container">
                        <input type="text" placeholder="Bộ lọc" id="rule-filter"/>
                        <div id="rule-filter-type"></div>
                    </div>
                    <div id="rule-filter-stats" class="filter-status">
                        Đang hiển thị&nbsp;<span id="displayed-rules"></span>&nbsp;trong số&nbsp;<span id="total-rules"></span>
                    </div>
                </div>
            </div>
            <div id="rule-details-panel">
                <div id="selected-rule-name-container" class="selected-rule-detail">
                    <div class="large-label">Tên quy tắc</div>
                    <div id="selected-rule-id" class="hidden"></div>
                    <div id="no-rule-selected-label" class="unset">Chưa chọn quy tắc nào.</div>
                    <input type="text" id="selected-rule-name" class="hidden"></input>
                </div>
                <div id="selected-rule-comments-container" class="selected-rule-detail">
                    <div class="large-label">Ghi chú quy tắc</div>
                    <textarea id="selected-rule-comments" rows="4" cols="60"></textarea>
                </div>
                <div id="selected-rule-conditions-container" class="selected-rule-detail">
                    <div class="large-label-container">
                        <div id="conditions-label" class="large-label">Điều kiện</div>
                        <div class="info fa fa-question-circle" title="Tất cả các điều kiện phải được đáp ứng để quy tắc này khớp."></div>
                        <button id="new-condition" title="Điều kiện mới" class="new-condition hidden fa fa-plus"></button>
                        <div class="clear"></div>
                    </div>
                    <div id="selected-rule-conditions"></div>
                </div>
                <div id="selected-rule-actions-container" class="selected-rule-detail">
                    <div class="large-label-container">
                        <div id="actions-label" class="large-label">Hành động</div>
                        <button id="new-action" title="Hành động mới" class="new-action hidden fa fa-plus"></button>
                        <div class="clear"></div>
                    </div>
                    <div id="selected-rule-actions"></div>
                </div>
                <div class="clear"></div>
            </div>
            <div id="message-and-save-container">
                <div id="message"></div>
                <div id="selected-rule-save" class="button hidden">Lưu</div>
            </div>
            <div class="clear"></div>
            <div id="glass-pane"></div>
            <div id="ok-dialog" class="small-dialog">
                <div id="ok-dialog-content" class="dialog-content"></div>
            </div>
            <div id="yes-no-dialog" class="small-dialog">
                <div id="yes-no-dialog-content" class="dialog-content"></div>
            </div>
            <div id="new-rule-dialog" class="small-dialog">
                <div class="dialog-content">
                    <div class="rule-setting">
                        <div class="setting-name">Tên quy tắc</div>
                        <div>
                            <input id="new-rule-name" type="text" />
                        </div>
                    </div>
                    <div class="rule-setting">
                        <div class="setting-name">Sao chép từ quy tắc hiện có (tùy chọn)</div>
                        <div>
                            <input id="copy-from-rule-name" placeholder="Tìm kiếm tên quy tắc" type="text" class="search" />
                        </div>
                    </div>
                </div>
            </div>
            <div id="new-condition-dialog" class="dialog">
                <div>
                    <div class="rule-setting">
                        <div class="setting-name">Biểu thức</div>
                        <div>
                            <div id="new-condition-expression"></div>
                        </div>
                    </div>
                </div>
                <div id="new-condition-button-container">
                    <div id="new-condition-add" class="button button-normal">Thêm</div>
                    <div id="new-condition-cancel" class="secondary-button button-normal">Hủy</div>
                    <div class="clear"></div>
                </div>
            </div>
            <div id="new-action-dialog" class="dialog">
                <div style="margin-bottom: 32px;">
                    <div class="rule-setting">
                        <div class="setting-name">Thuộc tính</div>
                        <div id="new-action-attribute-container">
                            <input id="new-action-attribute" type="text"></input>
                        </div>
                    </div>
                    <div class="rule-setting">
                        <div class="setting-name">Giá trị</div>
                        <div>
                            <div id="new-action-value"></div>
                        </div>
                    </div>
                </div>
                <div id="new-action-button-container">
                    <div id="new-action-add" class="button button-normal">Thêm</div>
                    <div id="new-action-cancel" class="secondary-button button-normal">Hủy</div>
                    <div class="clear"></div>
                </div>
            </div>
        </div>
    </body>
</html>
