package ec.edu.espe.banquito.routingservice.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.mapping.Document;
import java.time.LocalDateTime;

@Document(collection = "routing_payment_detail")
@CompoundIndex(name = "batch_line_unique", def = "{'batchId': 1, 'lineNumber': 1}", unique = true)
public class PaymentDetail {

    @Id
    private String id;

    private String batchId;
    private int lineNumber;
    private String transactionUuid;
    private String routingCode;
    private String accountDestination;
    private double amount;
    private String reference;
    private String beneficiaryName;
    private String beneficiaryEmail;

    private String status; // PROCESSING, PROCESSED, CLEARED, REJECTED
    private String errorCode;
    private String errorMessage;

    private LocalDateTime processedAt;

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getBatchId() { return batchId; }
    public void setBatchId(String batchId) { this.batchId = batchId; }

    public int getLineNumber() { return lineNumber; }
    public void setLineNumber(int lineNumber) { this.lineNumber = lineNumber; }

    public String getTransactionUuid() { return transactionUuid; }
    public void setTransactionUuid(String transactionUuid) { this.transactionUuid = transactionUuid; }

    public String getRoutingCode() { return routingCode; }
    public void setRoutingCode(String routingCode) { this.routingCode = routingCode; }

    public String getAccountDestination() { return accountDestination; }
    public void setAccountDestination(String accountDestination) { this.accountDestination = accountDestination; }

    public double getAmount() { return amount; }
    public void setAmount(double amount) { this.amount = amount; }

    public String getReference() { return reference; }
    public void setReference(String reference) { this.reference = reference; }

    public String getBeneficiaryName() { return beneficiaryName; }
    public void setBeneficiaryName(String beneficiaryName) { this.beneficiaryName = beneficiaryName; }

    public String getBeneficiaryEmail() { return beneficiaryEmail; }
    public void setBeneficiaryEmail(String beneficiaryEmail) { this.beneficiaryEmail = beneficiaryEmail; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public String getErrorCode() { return errorCode; }
    public void setErrorCode(String errorCode) { this.errorCode = errorCode; }

    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }

    public LocalDateTime getProcessedAt() { return processedAt; }
    public void setProcessedAt(LocalDateTime processedAt) { this.processedAt = processedAt; }
}
