package ec.edu.espe.banquito.routingservice.service;

import com.banquito.payswitch.notification.NotificationRequest;
import ec.edu.espe.banquito.banquitotariffservice.grpc.TariffCalculationGrpcRequest;
import ec.edu.espe.banquito.banquitotariffservice.grpc.TariffCalculationGrpcResponse;
import com.mongodb.client.result.UpdateResult;
import ec.edu.espe.banquito.routingservice.client.AccountCoreRestClient;
import ec.edu.espe.banquito.routingservice.client.NotificationGrpcClient;
import ec.edu.espe.banquito.routingservice.client.TariffGrpcClient;
import ec.edu.espe.banquito.routingservice.model.OffUsClearingMessage;
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

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
public class RoutingService {

    private static final Logger log = LoggerFactory.getLogger(RoutingService.class);


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

    @Value("${app.routing.local-completion-enabled:false}")
    private boolean localCompletionEnabled;

    // Sincroniza a los workers concurrentes del mismo batch con el resultado real
    // del débito inicial: solo el primero lo ejecuta, los demás esperan su resultado
    // en vez de asumir éxito y acreditar dinero que nunca se debitó.
    private final java.util.concurrent.ConcurrentHashMap<String, CompletableFuture<Boolean>> debitOutcomes =
            new java.util.concurrent.ConcurrentHashMap<>();

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

        // RF-03: Debitar el monto TOTAL declarado en el archivo al primer mensaje del lote.
        // Todos los workers del batch esperan el resultado real antes de acreditar nada:
        // si el débito falla (ej. fondos insuficientes), ninguna línea debe procesarse.
        if (!initialDebitIfNeeded(message)) {
            detail.setStatus("REJECTED");
            detail.setErrorCode("BATCH_DEBIT_FAILED");
            detail.setErrorMessage("No se acreditó: falló el débito inicial del monto total del lote (fondos insuficientes u otro error en la cuenta de origen).");
            detail.setProcessedAt(LocalDateTime.now());
            detailRepository.save(detail);
            updateBatchCounters(message, false);
            return;
        }

        // Evaluate routing code using the classification resolved by the parametric catalog
        String route = determineRoute(message);
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
                    OffUsClearingMessage clearingMessage = adaptForClearingHouse(message);
                    rabbitTemplate.convertAndSend(clearingQueue, clearingMessage);
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
                .setOnInsert("declaredTotalAmount", message.getDeclaredTotalAmount())
                .setOnInsert("successfulRecords", 0)
                .setOnInsert("rejectedRecords", 0)
                .setOnInsert("successfulAmount", 0.0)
                .setOnInsert("rejectedAmount", 0.0)
                .setOnInsert("refundAmount", 0.0)
                .setOnInsert("createdAt", LocalDateTime.now());
        mongoTemplate.upsert(query, update, PaymentBatch.class);
    }

    /**
     * RF-03: Débito inicial del monto total declarado en el archivo.
     * Usa una transición de estado atómica PROCESSING -> DEBITED para garantizar
     * que solo UN worker realice el débito, aunque lleguen múltiples líneas en paralelo.
     * Los demás workers del mismo batch ESPERAN el resultado real (vía CompletableFuture)
     * antes de decidir si acreditan su línea: si no se espera, una línea puede acreditarse
     * antes de que el worker ganador termine de confirmar que el débito falló, generando
     * dinero acreditado sin haber sido debitado nunca (bug detectado en pruebas).
     *
     * @return true si el débito inicial fue exitoso (o ya lo había sido); false si falló.
     */
    private boolean initialDebitIfNeeded(PaymentLineMessage message) {
        String batchId = message.getBatchId();
        CompletableFuture<Boolean> outcome = debitOutcomes.computeIfAbsent(batchId, id -> new CompletableFuture<>());

        // CAS atómico: solo el primer worker que gana esta actualización debita
        Query claimQuery = new Query(Criteria.where("batchId").is(batchId).and("status").is("PROCESSING"));
        Update claimUpdate = new Update().set("status", "DEBITED");
        UpdateResult claimed = mongoTemplate.updateFirst(claimQuery, claimUpdate, PaymentBatch.class);

        if (claimed.getModifiedCount() != 1) {
            // Otro worker ya está debitando (o ya terminó): esperar su resultado real
            try {
                return outcome.get(20, java.util.concurrent.TimeUnit.SECONDS);
            } catch (Exception e) {
                log.warn("[RF-03][DEBIT] No se pudo confirmar a tiempo el resultado del débito para batchId={}: {}",
                        batchId, e.getMessage());
                PaymentBatch current = mongoTemplate.findOne(
                        new Query(Criteria.where("batchId").is(batchId)), PaymentBatch.class);
                return current != null && !"FAILED".equals(current.getStatus());
            }
        }

        double totalAmount = message.getDeclaredTotalAmount();
        String debitAccount = (message.getOriginatingAccount() != null && !message.getOriginatingAccount().isBlank())
                ? message.getOriginatingAccount()
                : fallbackCorporateAccount;

        log.info("[RF-03][DEBIT] Debitando monto total declarado={} de cuenta={} para batchId={}",
                totalAmount, debitAccount, batchId);

        boolean success;
        try {
            // Débito total inicial: totalAmount=monto declarado, commissionAmount=0 (la comisión se cobra al cerrar)
            accountCoreClient.corporateDebit(batchId, debitAccount, totalAmount, 0.0);
            log.info("[RF-03][DEBIT] Débito inicial exitoso para batchId={}", batchId);
            success = true;
        } catch (Exception e) {
            // Si el débito falla (ej. fondos insuficientes), marcamos el batch como FAILED
            // con el motivo real, para que ninguna línea se acredite y el frontend pueda mostrarlo.
            log.error("[RF-03][DEBIT] Débito inicial fallido para batchId={}: {}", batchId, e.getMessage());
            Query failQuery = new Query(Criteria.where("batchId").is(batchId));
            Update failUpdate = new Update()
                    .set("status", "FAILED")
                    .set("failureReason", e.getMessage())
                    .set("updatedAt", LocalDateTime.now());
            mongoTemplate.updateFirst(failQuery, failUpdate, PaymentBatch.class);
            success = false;
        }

        outcome.complete(success);
        return success;
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

    // El estado final del lote refleja el resultado de las líneas (RF-02/RF-03), no
    // los pasos administrativos posteriores (comisión, devolución): un lote con todas
    // sus líneas acreditadas no debe pasar a FAILED solo porque el cobro de comisión falle.
    private String resolveOutcomeStatus(PaymentBatch batch) {
        if (batch.getSuccessfulRecords() <= 0) {
            return "FAILED";
        }
        if (batch.getRejectedRecords() > 0) {
            return "COMPLETED_WITH_ISSUES";
        }
        return "COMPLETED";
    }

    // RF-04: Comisión + devolución de rechazados cuando el lote está totalmente procesado
    private void completeBatch(PaymentBatch batch) {
        log.info("Completing batch: {}", batch.getBatchId());
        String outcomeStatus = resolveOutcomeStatus(batch);

        if (localCompletionEnabled) {
            Query q = new Query(Criteria.where("batchId").is(batch.getBatchId()));
            Update u = new Update()
                    .set("status", outcomeStatus)
                    .set("updatedAt", LocalDateTime.now())
                    .set("completedAt", LocalDateTime.now());
            mongoTemplate.updateFirst(q, u, PaymentBatch.class);
            log.info("Batch {} in local mode: {}", outcomeStatus, batch.getBatchId());
            return;
        }

        double refund = batch.getRejectedAmount();
        try {
            // RF-04: Calcular comisión solo sobre transacciones exitosas
            TariffCalculationGrpcRequest tariffReq = TariffCalculationGrpcRequest.newBuilder()
                    .setSuccessfulTx(batch.getSuccessfulRecords())
                    .setBatchId(batch.getBatchId())
                    .build();
            TariffCalculationGrpcResponse tariffResp = tariffClient.calculateTariff(tariffReq);

            String debitAccount = (batch.getOriginatingAccount() != null && !batch.getOriginatingAccount().isBlank())
                    ? batch.getOriginatingAccount()
                    : fallbackCorporateAccount;

            // RF-03: El débito inicial ya se hizo por el monto total.
            // Ahora solo registramos la comisión como débito adicional (si aplica).
            double commission = Double.parseDouble(tariffResp.getTotalCharge());
            if (commission > 0) {
                log.info("[RF-04][COMMISSION] Debitando comisión={} de cuenta={} para batchId={}",
                        commission, debitAccount, batch.getBatchId());
                accountCoreClient.corporateDebit(batch.getBatchId(), debitAccount, 0.0, commission);
            }

            // RF-03: Devolver el monto de líneas rechazadas a la cuenta corporativa
            if (refund > 0) {
                log.info("[RF-03][REFUND] Devolviendo monto rechazado={} a cuenta={} para batchId={}",
                        refund, debitAccount, batch.getBatchId());
                accountCoreClient.corporateRefund(batch.getBatchId(), debitAccount, refund);
            }
        } catch (Exception e) {
            // Falla en comisión/devolución: queda pendiente para revisión operativa,
            // pero no debe ocultar que las líneas del lote sí se procesaron.
            log.error("[RF-04] Cobro de comisión o devolución falló para batchId={} (lote permanece {}): {}",
                    batch.getBatchId(), outcomeStatus, e.getMessage());
        }

        Query q = new Query(Criteria.where("batchId").is(batch.getBatchId()));
        Update u = new Update()
                .set("status", outcomeStatus)
                .set("refundAmount", refund)
                .set("completedAt", LocalDateTime.now());
        mongoTemplate.updateFirst(q, u, PaymentBatch.class);

        log.info("[RF-03] Batch {}: batchId={}, exitosas={}, rechazadas={}, devuelto={}",
                outcomeStatus, batch.getBatchId(), batch.getSuccessfulRecords(), batch.getRejectedRecords(), refund);
    }

    private boolean tryClaimCompletion(String batchId) {
        // El lote ya debería estar en estado DEBITED tras el pago inicial de la primera línea
        Query query = new Query(Criteria.where("batchId").is(batchId).and("status").is("DEBITED"));
        Update update = new Update().set("status", "COMPLETING");
        UpdateResult result = mongoTemplate.updateFirst(query, update, PaymentBatch.class);
        return result.getModifiedCount() == 1;
    }

    // RF-03: Adapta el registro interno al formato esperado por la Cámara de Compensación
    // (clearinghouse-adapter) antes de publicarlo en la Cola de Salida.
    private OffUsClearingMessage adaptForClearingHouse(PaymentLineMessage message) {
        OffUsClearingMessage clearingMessage = new OffUsClearingMessage();
        clearingMessage.setBatchId(UUID.fromString(message.getBatchId()));
        clearingMessage.setTransactionId(resolveTransactionId(message.getTransactionUuid()));
        clearingMessage.setRoutingCode(message.getRoutingCode());
        clearingMessage.setOriginAccount(message.getOriginatingAccount());
        clearingMessage.setDestinationAccount(message.getAccountDestination());
        clearingMessage.setAmount(BigDecimal.valueOf(message.getAmount()));
        clearingMessage.setCurrency("USD");
        clearingMessage.setConcept(message.getReference());
        clearingMessage.setValueDate(LocalDate.now());
        return clearingMessage;
    }

    private UUID resolveTransactionId(String transactionUuid) {
        if (transactionUuid == null || transactionUuid.isBlank()) {
            return UUID.randomUUID();
        }
        try {
            return UUID.fromString(transactionUuid);
        } catch (IllegalArgumentException e) {
            return UUID.randomUUID();
        }
    }

    private String determineRoute(PaymentLineMessage message) {
        String classification = message.getRoutingClassification();
        if ("ON_US".equals(classification)) return "ONUS";
        if ("OFF_US".equals(classification)) return "OFFUS";
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
