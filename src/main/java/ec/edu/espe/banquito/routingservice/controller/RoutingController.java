package ec.edu.espe.banquito.routingservice.controller;

import ec.edu.espe.banquito.routingservice.dto.BatchStatusResponse;
import ec.edu.espe.banquito.routingservice.model.PaymentBatch;
import ec.edu.espe.banquito.routingservice.repository.PaymentBatchRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v2/payments/batches")
public class RoutingController {

    private final PaymentBatchRepository batchRepository;

    public RoutingController(PaymentBatchRepository batchRepository) {
        this.batchRepository = batchRepository;
    }

    // RF-06: Batch status endpoint consumed by Anthony (notification-service) via Kong
    @GetMapping("/{batchId}/status")
    public ResponseEntity<BatchStatusResponse> getBatchStatus(@PathVariable String batchId) {
        return batchRepository.findByBatchId(batchId)
                .map(this::toResponse)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    private BatchStatusResponse toResponse(PaymentBatch batch) {
        BatchStatusResponse r = new BatchStatusResponse();
        r.setBatchId(batch.getBatchId());
        r.setStatus(batch.getStatus());
        r.setDeclaredTotalRecords(batch.getDeclaredTotalRecords());
        r.setSuccessfulRecords(batch.getSuccessfulRecords());
        r.setRejectedRecords(batch.getRejectedRecords());
        r.setSuccessfulAmount(batch.getSuccessfulAmount());
        r.setRejectedAmount(batch.getRejectedAmount());
        r.setCreatedAt(batch.getCreatedAt());
        r.setUpdatedAt(batch.getUpdatedAt());
        r.setCompletedAt(batch.getCompletedAt());
        r.setFailureReason(batch.getFailureReason());
        return r;
    }
}
