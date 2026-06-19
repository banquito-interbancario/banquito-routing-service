package ec.edu.espe.banquito.routingservice.client;

import com.banquito.payswitch.notification.NotificationRequest;
import com.banquito.payswitch.notification.NotificationServiceGrpc;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Component;

@Component
public class NotificationGrpcClient {

    @GrpcClient("notification")
    private NotificationServiceGrpc.NotificationServiceBlockingStub blockingStub;

    public void sendNotification(NotificationRequest request) {
        blockingStub.sendNotification(request);
    }
}
