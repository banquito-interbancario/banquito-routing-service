package ec.edu.espe.banquito.routingservice.service;

import com.banquito.payswitch.notification.NotificationRequest;
import com.banquito.payswitch.tariff.TariffRequest;
import com.banquito.payswitch.tariff.TariffResponse;
import com.mongodb.client.result.UpdateResult;
import ec.edu.espe.banquito.routingservice.client.AccountCoreRestClient;
import ec.edu.espe.banquito.routingservice.client.NotificationGrpcClient;
import ec.edu.espe.banquito.routingservice.client.TariffGrpcClient;
import ec.edu.espe.banquito.routingservice.model.PaymentBatch;
import ec.edu.espe.banquito.routingservice.model.PaymentDetail;
import ec.edu.espe.banquito.routingservice.model.PaymentLineMessage;
import ec.edu.espe.banquito.routingservice.repository.PaymentDetailRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

@Service
public class RoutingService {

    private static final Logger log = LoggerFactory.getLogger(RoutingService.class);

    private static final String ONUS_CODE = "001";
    private static final Set<String> VALID_OFFUS_CODES = Set.of(
            "010", "017", "021", "023", "024", "025", "030", "034", "035", "036", "037", "041", "043"
    );

    private final PaymentDetailRepository detailRepository;
    private final MongoTemplate mongoTemplate;
    private final AccountCoreRestClient accountCoreClient;
    private final TariffGrpcClient tariffClient;
    private final NotificationGrpcClient notificationClient;
    private final RabbitTemplate rabbitTemplate;

    @Value("${app.rabbitmq.clearing-queue:clearing.outbound.queue}")
    private String clearingQueue;

    @Value("${account.core.corporate.account-number:0000000000}")
    private String fallbackCorporateAccount;

    public RoutingService(PaymentDetailRepository detailRepository,
                          MongoTemplate mongoTemplate,
                          AccountCoreRestClient accountCoreClient,
                          TariffGrpcClient tariffClient,
                          NotificationGrpcClient notificationClient,
                          RabbitTemplate rabbitTemplate) {
        this.detailRepository = detailRepository;
        this.mongoTemplate = mongoTemplate;
        this.accountCoreClient = accountCoreClient;
        this.tariffClient = tariffClient;
        this.notificationClient = notificationClient;
        this.rabbitTemplate = rabbitTemplate;
    }

    // RF-03: Consume payment lines with 5-20 concurrent workers
    @RabbitListener(queues = "${app.rabbitmq.payment-lines-queue}", concurrency = "5-20")
    public void processPaymentLine(PaymentLineMessage message) {
        log.info("Received payment line: batchId={}, lineNumber={}", message.getBatchId(), message.getLineNumber());

        // Idempotency: insert a PROCESSING record; duplicate key means already handled
        PaymentDetail detail = buildInitialDetail(message);
        try {
            detailRepository.save(detail);
        } catch (DuplicateKeyException e) {
            log.warn("Duplicate message ignored: batchId={}, lineNumber={}", message.getBatchId(), message.getLineNumber());
            return;
        }

        // Ensure the batch document exists in MongoDB (upsert, safe for concurrency)
        ensureBatchExists(message);

        // Evaluate routing code
        String route = determineRoute(message.getRoutingCode());
        boolean success = false;
        String errorCode = null;
        String errorMessage = null;

        switch (route) {
            case "ONUS" -> {
                try {
                    processOnUs(message, detail.getId());
                    success = true;
                } catch (Exception e) {
                    log.error("On-Us error batchId={} line={}: {}", message.getBatchId(), message.getLineNumber(), e.getMessage());
                    errorCode = "ONUS_PROCESSING_ERROR";
                    errorMessage = e.getMessage();
                }
            }
            case "OFFUS" -> {
                try {
                    rabbitTemplate.convertAndSend(clearingQueue, message);
                    success = true;
                } catch (Exception e) {
                    log.error("Off-Us routing error batchId={} line={}: {}", message.getBatchId(), message.getLineNumber(), e.getMessage());
                    errorCode = "OFFUS_ROUTING_ERROR";
                    errorMessage = e.getMessage();
                }
            }
            default -> {
                errorCode = "ROUTING_CODE_INVALID";
                errorMessage = "Invalid routing code: " + message.getRoutingCode();
                log.warn("Invalid routing code '{}' for batchId={}", message.getRoutingCode(), message.getBatchId());
            }
        }

        // Update the detail with its final status
        String finalStatus = success ? (route.equals("OFFUS") ? "CLEARED" : "PROCESSED") : "REJECTED";
        detail.setStatus(finalStatus);
        detail.setErrorCode(errorCode);
        detail.setErrorMessage(errorMessage);
        detail.setProcessedAt(LocalDateTime.now());
        detailRepository.save(detail);

        // Atomically update batch counters and check for completion
        updateBatchCounters(message, success);
    }

    private void processOnUs(PaymentLineMessage message, String detailId) {
        accountCoreClient.batchCredit(
                message.getBatchId(),
                message.getAccountDestination(),
                message.getAmount(),
                message.getReference(),
                message.getTransactionUuid()
        );

        // RF-03 / RF notification: fire-and-forget to Anthony
        sendNotificationAsync(message, detailId);
    }

    private void sendNotificationAsync(PaymentLineMessage message, String detailId) {
        CompletableFuture.runAsync(() -> {
            try {
                Map<String, String> vars = new HashMap<>();
                vars.put("beneficiaryName", message.getBeneficiaryName());
                vars.put("amount", String.format("%.2f", message.getAmount()));
                vars.put("reference", message.getReference());
                vars.put("batchId", message.getBatchId());

                NotificationRequest req = NotificationRequest.newBuilder()
                        .setPaymentDetailId(detailId != null ? Math.abs(detailId.hashCode()) : 0)
                        .setEmailTo(message.getBeneficiaryEmail())
                        .setSubject("Transferencia procesada exitosamente")
                        .setBodyTemplate("payment_credit_notification")
                        .putAllVariables(vars)
                        .build();

                notificationClient.sendNotification(req);
            } catch (Exception e) {
                log.warn("Notification failed batchId={} line={}: {}", message.getBatchId(), message.getLineNumber(), e.getMessage());
            }
        });
    }

    private void ensureBatchExists(PaymentLineMessage message) {
        Query query = new Query(Criteria.where("batchId").is(message.getBatchId()));
        Update update = new Update()
                .setOnInsert("batchId", message.getBatchId())
                .setOnInsert("status", "PROCESSING")
                .setOnInsert("originatingAccount", message.getOriginatingAccount())
                .setOnInsert("declaredTotalRecords", message.getDeclaredTotalRecords())
                .setOnInsert("successfulRecords", 0)
                .setOnInsert("rejectedRecords", 0)
                .setOnInsert("successfulAmount", 0.0)
                .setOnInsert("rejectedAmount", 0.0)
                .setOnInsert("createdAt", LocalDateTime.now());
        mongoTemplate.upsert(query, update, PaymentBatch.class);
    }

    private void updateBatchCounters(PaymentLineMessage message, boolean success) {
        Query query = new Query(Criteria.where("batchId").is(message.getBatchId()));
        Update update = new Update().set("updatedAt", LocalDateTime.now());

        if (success) {
            update.inc("successfulRecords", 1).inc("successfulAmount", message.getAmount());
        } else {
            update.inc("rejectedRecords", 1).inc("rejectedAmount", message.getAmount());
        }

        FindAndModifyOptions opts = FindAndModifyOptions.options().returnNew(true);
        PaymentBatch updated = mongoTemplate.findAndModify(query, update, opts, PaymentBatch.class);

        if (updated != null && updated.getDeclaredTotalRecords() > 0) {
            int processed = updated.getSuccessfulRecords() + updated.getRejectedRecords();
            if (processed >= updated.getDeclaredTotalRecords()) {
                // Atomic CAS: only the worker that wins this update triggers completion
                if (tryClaimCompletion(updated.getBatchId())) {
                    completeBatch(updated);
                }
            }
        }
    }

    // RF-04: Tariff calculation + corporate debit when the batch is fully processed
    private void completeBatch(PaymentBatch batch) {
        log.info("Completing batch: {}", batch.getBatchId());
        try {
            TariffRequest tariffReq = TariffRequest.newBuilder()
                    .setSuccessfulTx(batch.getSuccessfulRecords())
                    .setBatchId(batch.getBatchId())
                    .build();
            TariffResponse tariffResp = tariffClient.calculateTariff(tariffReq);

            String debitAccount = (batch.getOriginatingAccount() != null && !batch.getOriginatingAccount().isBlank())
                    ? batch.getOriginatingAccount()
                    : fallbackCorporateAccount;

            accountCoreClient.corporateDebit(
                    batch.getBatchId(),
                    debitAccount,
                    batch.getSuccessfulAmount(),
                    tariffResp.getTotalCharge()
            );

            Query q = new Query(Criteria.where("batchId").is(batch.getBatchId()));
            Update u = new Update().set("status", "COMPLETED").set("completedAt", LocalDateTime.now());
            mongoTemplate.updateFirst(q, u, PaymentBatch.class);

            log.info("Batch COMPLETED: {}", batch.getBatchId());
        } catch (Exception e) {
            log.error("Batch completion failed {}: {}", batch.getBatchId(), e.getMessage());
            Query q = new Query(Criteria.where("batchId").is(batch.getBatchId()));
            Update u = new Update().set("status", "FAILED").set("updatedAt", LocalDateTime.now());
            mongoTemplate.updateFirst(q, u, PaymentBatch.class);
        }
    }

    private boolean tryClaimCompletion(String batchId) {
        Query query = new Query(Criteria.where("batchId").is(batchId).and("status").is("PROCESSING"));
        Update update = new Update().set("status", "COMPLETING");
        UpdateResult result = mongoTemplate.updateFirst(query, update, PaymentBatch.class);
        return result.getModifiedCount() == 1;
    }

    private String determineRoute(String routingCode) {
        if (ONUS_CODE.equals(routingCode)) return "ONUS";
        if (VALID_OFFUS_CODES.contains(routingCode)) return "OFFUS";
        return "INVALID";
    }

    private PaymentDetail buildInitialDetail(PaymentLineMessage msg) {
        PaymentDetail d = new PaymentDetail();
        d.setBatchId(msg.getBatchId());
        d.setLineNumber(msg.getLineNumber());
        d.setTransactionUuid(msg.getTransactionUuid());
        d.setRoutingCode(msg.getRoutingCode());
        d.setAccountDestination(msg.getAccountDestination());
        d.setAmount(msg.getAmount());
        d.setReference(msg.getReference());
        d.setBeneficiaryName(msg.getBeneficiaryName());
        d.setBeneficiaryEmail(msg.getBeneficiaryEmail());
        d.setStatus("PROCESSING");
        return d;
    }

}
