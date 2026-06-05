package ec.edu.espe.banquito.routingservice.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PaymentLineMessage {

    @JsonProperty("batch_id")
    private String batchId;

    @JsonProperty("line_number")
    private int lineNumber;

    @JsonProperty("routing_code")
    private String routingCode;

    @JsonProperty("account_destination")
    private String accountDestination;

    private double amount;

    private String reference;

    @JsonProperty("beneficiary_name")
    private String beneficiaryName;

    @JsonProperty("beneficiary_email")
    private String beneficiaryEmail;

    // Alan no envía este campo — queda en null, no afecta el procesamiento
    @JsonProperty("transaction_uuid")
    private String transactionUuid;

    // Alan no envía este campo — sin él el batch queda en PROCESSING indefinidamente
    // Workaround: se maneja en RoutingService con declaredTotalRecords=0
    @JsonProperty("declared_total_records")
    private int declaredTotalRecords;

    public String getBatchId() { return batchId; }
    public void setBatchId(String batchId) { this.batchId = batchId; }

    public int getLineNumber() { return lineNumber; }
    public void setLineNumber(int lineNumber) { this.lineNumber = lineNumber; }

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

    public String getTransactionUuid() { return transactionUuid; }
    public void setTransactionUuid(String transactionUuid) { this.transactionUuid = transactionUuid; }

    public int getDeclaredTotalRecords() { return declaredTotalRecords; }
    public void setDeclaredTotalRecords(int declaredTotalRecords) { this.declaredTotalRecords = declaredTotalRecords; }
}
