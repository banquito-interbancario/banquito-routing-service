package ec.edu.espe.banquito.routingservice.grpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class PaymentLineIngestionGrpcServer {

    private final int port;
    private final PaymentLineIngestionGrpcService paymentLineIngestionGrpcService;
    private Server server;

    public PaymentLineIngestionGrpcServer(
            @Value("${routing.grpc.port:9094}") int port,
            PaymentLineIngestionGrpcService paymentLineIngestionGrpcService) {
        this.port = port;
        this.paymentLineIngestionGrpcService = paymentLineIngestionGrpcService;
    }

    @PostConstruct
    public void start() throws IOException {
        server = ServerBuilder
                .forPort(port)
                .addService(paymentLineIngestionGrpcService)
                .build()
                .start();
    }

    @PreDestroy
    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }
}
