package ec.edu.espe.banquito.routingservice.dto;

import java.time.LocalDateTime;

public class BatchStatusResponse {

    private String batchId;
    private String status;
    private int declaredTotalRecords;
    private int successfulRecords;
    private int rejectedRecords;
    private double successfulAmount;
    private double rejectedAmount;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
    private LocalDateTime completedAt;
    private String failureReason;

    public String getBatchId() { return batchId; }
    public void setBatchId(String batchId) { this.batchId = batchId; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public int getDeclaredTotalRecords() { return declaredTotalRecords; }
    public void setDeclaredTotalRecords(int declaredTotalRecords) { this.declaredTotalRecords = declaredTotalRecords; }

    public int getSuccessfulRecords() { return successfulRecords; }
    public void setSuccessfulRecords(int successfulRecords) { this.successfulRecords = successfulRecords; }

    public int getRejectedRecords() { return rejectedRecords; }
    public void setRejectedRecords(int rejectedRecords) { this.rejectedRecords = rejectedRecords; }

    public double getSuccessfulAmount() { return successfulAmount; }
    public void setSuccessfulAmount(double successfulAmount) { this.successfulAmount = successfulAmount; }

    public double getRejectedAmount() { return rejectedAmount; }
    public void setRejectedAmount(double rejectedAmount) { this.rejectedAmount = rejectedAmount; }

    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }

    public LocalDateTime getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(LocalDateTime updatedAt) { this.updatedAt = updatedAt; }

    public LocalDateTime getCompletedAt() { return completedAt; }
    public void setCompletedAt(LocalDateTime completedAt) { this.completedAt = completedAt; }

    public String getFailureReason() { return failureReason; }
    public void setFailureReason(String failureReason) { this.failureReason = failureReason; }
}
