package ec.edu.espe.banquito.routingservice.client;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Component
public class AccountCoreRestClient {

    private final RestTemplate restTemplate = new RestTemplate();

    @Value("${CORE_GATEWAY_URL:http://account-core-service:8081}")
    private String baseUrl;

    public void batchCredit(String batchId, String accountDestination, double amount,
                            String reference, String transactionUuid) {

        String txUuid = (transactionUuid != null && !transactionUuid.isBlank())
                ? transactionUuid
                : UUID.randomUUID().toString();

        Map<String, Object> credit = new HashMap<>();
        credit.put("accountNumber", accountDestination);
        credit.put("amount", amount);
        credit.put("reference", reference);
        credit.put("transactionUuid", txUuid);

        Map<String, Object> body = new HashMap<>();
        body.put("batchId", batchId);
        body.put("credits", List.of(credit));

        restTemplate.postForEntity(baseUrl + "/api/v2/payments/batch-credit", body, Void.class);
    }

    public void corporateDebit(String batchId, String accountNumber,
                               double totalAmount, double commissionAmount) {
        Map<String, Object> body = new HashMap<>();
        body.put("batchId", batchId);
        body.put("accountNumber", accountNumber);
        body.put("transactionUuid", UUID.randomUUID().toString());
        body.put("totalAmount", totalAmount);
        body.put("commissionAmount", commissionAmount);

        restTemplate.postForEntity(baseUrl + "/api/v2/payments/corporate-debit", body, Void.class);
    }

    /**
     * RF-03: Devuelve a la cuenta corporativa el monto de las líneas rechazadas.
     * Se llama al final del batch si rejectedAmount > 0.
     */
    public void corporateRefund(String batchId, String accountNumber, double refundAmount) {
        Map<String, Object> body = new HashMap<>();
        body.put("batchId", batchId);
        body.put("accountNumber", accountNumber);
        body.put("transactionUuid", UUID.randomUUID().toString());
        body.put("refundAmount", refundAmount);

        restTemplate.postForEntity(baseUrl + "/api/v2/payments/corporate-refund", body, Void.class);
    }
}
