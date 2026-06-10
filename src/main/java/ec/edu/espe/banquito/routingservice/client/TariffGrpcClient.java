package ec.edu.espe.banquito.routingservice.client;

import ec.edu.espe.banquito.banquitotariffservice.grpc.TariffCalculationGrpcRequest;
import ec.edu.espe.banquito.banquitotariffservice.grpc.TariffCalculationGrpcResponse;
import ec.edu.espe.banquito.banquitotariffservice.grpc.TariffGrpcServiceGrpc;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Component;
import java.util.concurrent.TimeUnit;

@Component
public class TariffGrpcClient {

    @GrpcClient("tariff")
    private TariffGrpcServiceGrpc.TariffGrpcServiceBlockingStub blockingStub;

    public TariffCalculationGrpcResponse calculateTariff(TariffCalculationGrpcRequest request) {
        return blockingStub
                .withDeadlineAfter(5, TimeUnit.SECONDS)
                .calculateTariff(request);
    }
}
