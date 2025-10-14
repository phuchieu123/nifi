/*
 * Được cấp phép cho Apache Software Foundation (ASF) theo một hoặc nhiều
 * thỏa thuận cấp phép của người đóng góp. Xem tệp NOTICE đi kèm với
 * công trình này để biết thêm thông tin về quyền sở hữu bản quyền.
 * ASF cấp phép cho bạn sử dụng tệp này theo Giấy phép Apache, Phiên bản 2.0
 * (the "License"); bạn không được sử dụng tệp này trừ khi tuân thủ Giấy phép.
 * Bạn có thể lấy một bản sao của Giấy phép tại:
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Trừ khi được yêu cầu bởi luật pháp hiện hành hoặc có thỏa thuận bằng văn bản,
 * phần mềm được phân phối theo Giấy phép này được cung cấp "NGUYÊN TRẠNG",
 * KHÔNG CÓ BẤT KỲ BẢO HÀNH NÀO, dù rõ ràng hay ngụ ý.
 * Xem Giấy phép để biết thêm chi tiết về quyền và giới hạn của bạn.
 */

package org.apache.nifi.flow;

import java.util.List;
import java.util.Set;

import io.swagger.annotations.ApiModelProperty;

public class VersionedConnection extends VersionedComponent {
    private ConnectableComponent source;
    private ConnectableComponent destination;
    private Integer labelIndex;
    private Long zIndex;
    private Set<String> selectedRelationships;

    private Long backPressureObjectThreshold;
    private String backPressureDataSizeThreshold;
    private String flowFileExpiration;
    private List<String> prioritizers;
    private List<Position> bends;

    private String loadBalanceStrategy;
    private String partitioningAttribute;
    private String loadBalanceCompression;


    @ApiModelProperty("Nguồn (source) của kết nối.")
    public ConnectableComponent getSource() {
        return source;
    }

    public void setSource(ConnectableComponent source) {
        this.source = source;
    }

    @ApiModelProperty("Đích đến (destination) của kết nối.")
    public ConnectableComponent getDestination() {
        return destination;
    }

    public void setDestination(ConnectableComponent destination) {
        this.destination = destination;
    }

    @ApiModelProperty("Các điểm uốn (bend points) trên đường kết nối.")
    public List<Position> getBends() {
        return bends;
    }

    public void setBends(List<Position> bends) {
        this.bends = bends;
    }

    @ApiModelProperty("Chỉ số của điểm uốn nơi nhãn (label) của kết nối được đặt.")
    public Integer getLabelIndex() {
        return labelIndex;
    }

    public void setLabelIndex(Integer labelIndex) {
        this.labelIndex = labelIndex;
    }

    @ApiModelProperty(
            value = "Chỉ số z (z-index) của kết nối.",
            name = "zIndex")  // Jackson ánh xạ tên phương thức này thành khóa JSON "zIndex", nhưng Swagger thì không mặc định làm vậy
    public Long getzIndex() {
        return zIndex;
    }

    public void setzIndex(Long zIndex) {
        this.zIndex = zIndex;
    }

    @ApiModelProperty("Danh sách các quan hệ (relationship) được chọn tạo thành kết nối này.")
    public Set<String> getSelectedRelationships() {
        return selectedRelationships;
    }

    public void setSelectedRelationships(Set<String> relationships) {
        this.selectedRelationships = relationships;
    }

    @ApiModelProperty("Ngưỡng số lượng đối tượng (object count) dùng để xác định khi nào áp dụng cơ chế chặn luồng (back pressure). Việc cập nhật giá trị này là thay đổi thụ động, nghĩa là nó "
        + "không ảnh hưởng đến các tệp đã vượt ngưỡng, nhưng giúp các bộ xử lý đầu vào (feeder processors) dừng việc đẩy thêm dữ liệu vào hàng đợi công việc.")
    public Long getBackPressureObjectThreshold() {
        return backPressureObjectThreshold;
    }

    public void setBackPressureObjectThreshold(Long backPressureObjectThreshold) {
        this.backPressureObjectThreshold = backPressureObjectThreshold;
    }

    @ApiModelProperty("Ngưỡng kích thước dữ liệu (data size) dùng để xác định khi nào áp dụng cơ chế chặn luồng (back pressure). Việc cập nhật giá trị này là thay đổi thụ động, "
        + "không ảnh hưởng đến các tệp đã vượt ngưỡng, nhưng giúp các bộ xử lý đầu vào dừng việc gửi quá nhiều dữ liệu vào hàng đợi.")
    public String getBackPressureDataSizeThreshold() {
        return backPressureDataSizeThreshold;
    }

    public void setBackPressureDataSizeThreshold(String backPressureDataSizeThreshold) {
        this.backPressureDataSizeThreshold = backPressureDataSizeThreshold;
    }

    @ApiModelProperty("Thời gian tối đa mà một FlowFile có thể tồn tại trong luồng trước khi bị loại bỏ tự động. "
        + "Khi một FlowFile đạt đến giới hạn thời gian này, nó sẽ bị chấm dứt (terminated) khi bộ xử lý tiếp theo cố gắng xử lý nó.")
    public String getFlowFileExpiration() {
        return flowFileExpiration;
    }

    public void setFlowFileExpiration(String flowFileExpiration) {
        this.flowFileExpiration = flowFileExpiration;
    }

    @ApiModelProperty("Các bộ so sánh (comparator) được sử dụng để sắp xếp ưu tiên trong hàng đợi.")
    public List<String> getPrioritizers() {
        return prioritizers;
    }

    public void setPrioritizers(List<String> prioritizers) {
        this.prioritizers = prioritizers;
    }

    @ApiModelProperty(value = "Chiến lược (Strategy) được sử dụng để cân bằng tải dữ liệu giữa các nút trong cụm, hoặc null nếu chưa được chỉ định.",
            allowableValues = "DO_NOT_LOAD_BALANCE, PARTITION_BY_ATTRIBUTE, ROUND_ROBIN, SINGLE_NODE")
    public String getLoadBalanceStrategy() {
        return loadBalanceStrategy;
    }

    public void setLoadBalanceStrategy(String loadBalanceStrategy) {
        this.loadBalanceStrategy = loadBalanceStrategy;
    }

    @ApiModelProperty("Thuộc tính được sử dụng để phân vùng dữ liệu khi cân bằng tải trên cụm. "
            + "Nếu Load Balance Strategy được cấu hình là PARTITION_BY_ATTRIBUTE, giá trị trả về của phương thức này "
            + "là tên của thuộc tính FlowFile được dùng để xác định nút nào trong cụm sẽ nhận FlowFile đó. "
            + "Nếu chiến lược cân bằng tải không được đặt hoặc được đặt khác, thuộc tính này sẽ không có tác dụng.")
    public String getPartitioningAttribute() {
        return partitioningAttribute;
    }

    public void setPartitioningAttribute(final String partitioningAttribute) {
        this.partitioningAttribute = partitioningAttribute;
    }

    @ApiModelProperty(value = "Xác định có nên nén dữ liệu hay không khi truyền FlowFile giữa các nút trong cụm.",
            allowableValues = "DO_NOT_COMPRESS, COMPRESS_ATTRIBUTES_ONLY, COMPRESS_ATTRIBUTES_AND_CONTENT")
    public String getLoadBalanceCompression() {
        return loadBalanceCompression;
    }

    public void setLoadBalanceCompression(final String compression) {
        this.loadBalanceCompression = compression;
    }

    @Override
    public ComponentType getComponentType() {
        return ComponentType.CONNECTION;
    }
}
