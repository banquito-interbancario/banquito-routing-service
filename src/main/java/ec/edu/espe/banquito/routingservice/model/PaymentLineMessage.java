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

    @JsonProperty("transaction_uuid")
    private String transactionUuid;

    @JsonProperty("declared_total_records")
    private int declaredTotalRecords;

    // cuenta_matriz del CSV. Alan debe agregar este campo al mensaje RabbitMQ.
    // Si no viene, corporateDebit usará CORPORATE_ACCOUNT_NUMBER del properties.
    @JsonProperty("originating_account")
    private String originatingAccount;

    // Monto total declarado en la cabecera del archivo (para débito inicial)
    @JsonProperty("declared_total_amount")
    private double declaredTotalAmount;

    // ON_US = local (BanQuito), OFF_US = externa. Viene del catálogo paramétrico del file-reception-service.
    @JsonProperty("routing_classification")
    private String routingClassification;

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

    public String getOriginatingAccount() { return originatingAccount; }
    public void setOriginatingAccount(String originatingAccount) { this.originatingAccount = originatingAccount; }

    public double getDeclaredTotalAmount() { return declaredTotalAmount; }
    public void setDeclaredTotalAmount(double declaredTotalAmount) { this.declaredTotalAmount = declaredTotalAmount; }

    public String getRoutingClassification() { return routingClassification; }
    public void setRoutingClassification(String routingClassification) { this.routingClassification = routingClassification; }
}
