package ec.edu.espe.banquito.routingservice.grpc;

import ec.edu.espe.banquito.routingservice.model.PaymentLineMessage;
import ec.edu.espe.banquito.routingservice.service.RoutingService;
import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class PaymentLineIngestionGrpcService extends PaymentLineIngestionServiceGrpc.PaymentLineIngestionServiceImplBase {

    private final RoutingService routingService;

    public PaymentLineIngestionGrpcService(RoutingService routingService) {
        this.routingService = routingService;
    }

    @Override
    public void publishBatchLines(PublishBatchLinesRequest request,
                                  StreamObserver<PublishBatchLinesResponse> responseObserver) {
        int declaredTotalRecords = request.getLinesCount();

        for (BatchLine line : request.getLinesList()) {
            routingService.processPaymentLine(toMessage(request.getBatchId(), line, declaredTotalRecords));
        }

        responseObserver.onNext(PublishBatchLinesResponse.newBuilder()
                .setAccepted(true)
                .setMessage("Lines accepted by routing-service")
                .build());
        responseObserver.onCompleted();
    }

    private PaymentLineMessage toMessage(String requestBatchId, BatchLine line, int declaredTotalRecords) {
        String batchId = !line.getBatchId().isBlank() ? line.getBatchId() : requestBatchId;

        PaymentLineMessage message = new PaymentLineMessage();
        message.setBatchId(batchId);
        message.setLineNumber(line.getLineNumber());
        message.setRoutingCode(line.getRoutingCode());
        message.setAccountDestination(line.getAccountDestination());
        message.setAmount(Double.parseDouble(line.getAmount()));
        message.setReference(line.getReference());
        message.setBeneficiaryName(line.getBeneficiaryName());
        message.setBeneficiaryEmail(line.getBeneficiaryEmail());
        message.setDeclaredTotalRecords(declaredTotalRecords);
        message.setTransactionUuid(UUID.randomUUID().toString());
        return message;
    }
}
