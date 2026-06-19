package ec.edu.espe.banquito.routingservice.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import java.time.LocalDateTime;

@Document(collection = "routing_payment_batch")
public class PaymentBatch {

    @Id
    private String id;

    @Indexed(unique = true)
    private String batchId;

    private String status; // PROCESSING, DEBITED, COMPLETING, COMPLETED, FAILED

    private String originatingAccount;

    private int declaredTotalRecords;
    private int successfulRecords;
    private int rejectedRecords;
    private double successfulAmount;
    private double rejectedAmount;
    private double declaredTotalAmount;  // monto total debitado al inicio (del header del archivo)
    private double refundAmount;         // monto devuelto al final (= rejectedAmount)

    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
    private LocalDateTime completedAt;

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

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

    public double getDeclaredTotalAmount() { return declaredTotalAmount; }
    public void setDeclaredTotalAmount(double declaredTotalAmount) { this.declaredTotalAmount = declaredTotalAmount; }

    public double getRefundAmount() { return refundAmount; }
    public void setRefundAmount(double refundAmount) { this.refundAmount = refundAmount; }

    public String getOriginatingAccount() { return originatingAccount; }
    public void setOriginatingAccount(String originatingAccount) { this.originatingAccount = originatingAccount; }

    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }

    public LocalDateTime getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(LocalDateTime updatedAt) { this.updatedAt = updatedAt; }

    public LocalDateTime getCompletedAt() { return completedAt; }
    public void setCompletedAt(LocalDateTime completedAt) { this.completedAt = completedAt; }
}
