package ec.edu.espe.banquito.routingservice.model;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.UUID;

/**
 * RF-03: Registro adaptado para la Cámara de Compensación.
 * El Switch transforma el PaymentLineMessage interno a este formato antes de
 * publicarlo en la Cola de Salida (clearing.outbound.queue), ya que el
 * clearinghouse-adapter espera esta forma exacta (UUIDs, BigDecimal, moneda, etc).
 */
public class OffUsClearingMessage {

    private UUID batchId;
    private UUID transactionId;
    private String routingCode;
    private String originAccount;
    private String destinationAccount;
    private BigDecimal amount;
    private String currency;
    private String concept;
    private LocalDate valueDate;

    public UUID getBatchId() { return batchId; }
    public void setBatchId(UUID batchId) { this.batchId = batchId; }

    public UUID getTransactionId() { return transactionId; }
    public void setTransactionId(UUID transactionId) { this.transactionId = transactionId; }

    public String getRoutingCode() { return routingCode; }
    public void setRoutingCode(String routingCode) { this.routingCode = routingCode; }

    public String getOriginAccount() { return originAccount; }
    public void setOriginAccount(String originAccount) { this.originAccount = originAccount; }

    public String getDestinationAccount() { return destinationAccount; }
    public void setDestinationAccount(String destinationAccount) { this.destinationAccount = destinationAccount; }

    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }

    public String getCurrency() { return currency; }
    public void setCurrency(String currency) { this.currency = currency; }

    public String getConcept() { return concept; }
    public void setConcept(String concept) { this.concept = concept; }

    public LocalDate getValueDate() { return valueDate; }
    public void setValueDate(LocalDate valueDate) { this.valueDate = valueDate; }
}
