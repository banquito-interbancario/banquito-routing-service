package ec.edu.espe.banquito.routingservice.client;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import java.util.HashMap;
import java.util.Map;

@Component
public class AccountCoreRestClient {

    private final RestTemplate restTemplate = new RestTemplate();

    @Value("${account.core.rest.url:http://account-core-service:8080}")
    private String baseUrl;

    public void batchCredit(String batchId, String accountDestination, double amount,
                            String reference, String transactionUuid) {
        Map<String, Object> body = new HashMap<>();
        body.put("batchId", batchId);
        body.put("accountDestination", accountDestination);
        body.put("amount", amount);
        body.put("reference", reference);
        body.put("transactionUuid", transactionUuid);
        restTemplate.postForEntity(baseUrl + "/api/v1/accounts/batch-credit", body, Void.class);
    }

    public void corporateDebit(String batchId, double totalAmount, double commissionAmount) {
        Map<String, Object> body = new HashMap<>();
        body.put("batchId", batchId);
        body.put("totalAmount", totalAmount);
        body.put("commissionAmount", commissionAmount);
        restTemplate.postForEntity(baseUrl + "/api/v1/accounts/corporate-debit", body, Void.class);
    }
}
