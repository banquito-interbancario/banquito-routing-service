package ec.edu.espe.banquito.routingservice.service;

import ec.edu.espe.banquito.routingservice.client.AccountCoreRestClient;
import ec.edu.espe.banquito.routingservice.client.NotificationGrpcClient;
import ec.edu.espe.banquito.routingservice.client.TariffGrpcClient;
import ec.edu.espe.banquito.routingservice.model.PaymentBatch;
import ec.edu.espe.banquito.routingservice.model.PaymentDetail;
import ec.edu.espe.banquito.routingservice.model.PaymentLineMessage;
import ec.edu.espe.banquito.routingservice.repository.PaymentDetailRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.ArrayList;
import java.util.List;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class RoutingServiceTest {

    @Mock
    private PaymentDetailRepository detailRepository;

    @Mock
    private MongoTemplate mongoTemplate;

    @Mock
    private AccountCoreRestClient accountCoreClient;

    @Mock
    private TariffGrpcClient tariffClient;

    @Mock
    private NotificationGrpcClient notificationClient;

    @Mock
    private RabbitTemplate rabbitTemplate;

    @InjectMocks
    private RoutingService routingService;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(routingService, "clearingQueue", "clearing.outbound.queue");
        ReflectionTestUtils.setField(routingService, "fallbackCorporateAccount", "0000000000");
        ReflectionTestUtils.setField(routingService, "localCompletionEnabled", false);

        when(detailRepository.save(any(PaymentDetail.class))).thenAnswer(inv -> inv.getArgument(0));

        PaymentBatch batch = new PaymentBatch();
        batch.setBatchId("batch-001");
        batch.setDeclaredTotalRecords(0);
        when(mongoTemplate.findAndModify(any(Query.class), any(Update.class),
                any(FindAndModifyOptions.class), eq(PaymentBatch.class)))
                .thenReturn(batch);
    }

    @Test
    void processPaymentLine_debeEnrutarComoONUS_cuandoCodigoEs001() {
        PaymentLineMessage message = buildMessage("batch-001", 1, "001", "0009876543", 500.0);

        routingService.processPaymentLine(message);

        verify(accountCoreClient).batchCredit(
                eq("batch-001"), eq("0009876543"), eq(500.0), any(), any());

        ArgumentCaptor<PaymentDetail> captor = ArgumentCaptor.forClass(PaymentDetail.class);
        verify(detailRepository, atLeast(2)).save(captor.capture());
        PaymentDetail finalDetail = captor.getAllValues().get(captor.getAllValues().size() - 1);
        assertThat(finalDetail.getStatus()).isEqualTo("PROCESSED");
    }

    @Test
    void processPaymentLine_debeEnrutarComoOFFUS_cuandoCodigoEs002() {
        PaymentLineMessage message = buildMessage("batch-001", 2, "002", "0009999999", 200.0);

        routingService.processPaymentLine(message);

        verify(rabbitTemplate).convertAndSend(eq("clearing.outbound.queue"), eq(message));

        ArgumentCaptor<PaymentDetail> captor = ArgumentCaptor.forClass(PaymentDetail.class);
        verify(detailRepository, atLeast(2)).save(captor.capture());
        PaymentDetail finalDetail = captor.getAllValues().get(captor.getAllValues().size() - 1);
        assertThat(finalDetail.getStatus()).isEqualTo("CLEARED");
    }

    @Test
    void processPaymentLine_debeRechazar_cuandoCodigoEsInvalido() {
        PaymentLineMessage message = buildMessage("batch-001", 3, "999", "0009999999", 100.0);

        routingService.processPaymentLine(message);

        verifyNoInteractions(accountCoreClient);
        verifyNoInteractions(rabbitTemplate);

        ArgumentCaptor<PaymentDetail> captor = ArgumentCaptor.forClass(PaymentDetail.class);
        verify(detailRepository, atLeast(2)).save(captor.capture());
        PaymentDetail finalDetail = captor.getAllValues().get(captor.getAllValues().size() - 1);
        assertThat(finalDetail.getStatus()).isEqualTo("REJECTED");
        assertThat(finalDetail.getErrorCode()).isEqualTo("ROUTING_CODE_INVALID");
    }

    @Test
    void processPaymentLine_debeIgnorarMensajeDuplicado_cuandoHayDuplicateKey() {
        PaymentLineMessage message = buildMessage("batch-001", 1, "001", "0009876543", 500.0);
        when(detailRepository.save(any(PaymentDetail.class)))
                .thenThrow(new DuplicateKeyException("duplicate batch_line_unique"));

        routingService.processPaymentLine(message);

        verifyNoInteractions(accountCoreClient);
        verifyNoInteractions(mongoTemplate);
    }

    @Test
    void processPaymentLine_debeGuardarDetalleInicial_conEstadoPROCESSING() {
        // Captura el status en el momento exacto de cada save (el objeto es mutable)
        List<String> statusesPorOrden = new ArrayList<>();
        when(detailRepository.save(any(PaymentDetail.class))).thenAnswer(inv -> {
            PaymentDetail d = inv.getArgument(0);
            statusesPorOrden.add(d.getStatus());
            return d;
        });

        PaymentLineMessage message = buildMessage("batch-001", 5, "001", "0001234567", 300.0);
        routingService.processPaymentLine(message);

        assertThat(statusesPorOrden).isNotEmpty();
        assertThat(statusesPorOrden.get(0)).isEqualTo("PROCESSING"); // primer save
        assertThat(statusesPorOrden.get(statusesPorOrden.size() - 1)).isEqualTo("PROCESSED"); // final

        verify(detailRepository, atLeast(2)).save(argThat(d ->
                "batch-001".equals(d.getBatchId()) && d.getLineNumber() == 5));
    }

    @Test
    void processPaymentLine_debeEnrutarComoOFFUS_paratodosLosCodigos() {
        String[] offusCodes = {"003", "004", "005", "010", "017", "021", "023"};
        for (String code : offusCodes) {
            PaymentLineMessage message = buildMessage("batch-002", 1, code, "0009999999", 50.0);
            routingService.processPaymentLine(message);
        }
        verify(rabbitTemplate, times(offusCodes.length))
                .convertAndSend(eq("clearing.outbound.queue"), any(PaymentLineMessage.class));
    }

    private PaymentLineMessage buildMessage(String batchId, int lineNumber,
                                            String routingCode, String accountDest, double amount) {
        PaymentLineMessage msg = new PaymentLineMessage();
        msg.setBatchId(batchId);
        msg.setLineNumber(lineNumber);
        msg.setRoutingCode(routingCode);
        msg.setAccountDestination(accountDest);
        msg.setAmount(amount);
        msg.setReference("REF-" + lineNumber);
        msg.setBeneficiaryName("Beneficiario Test");
        msg.setBeneficiaryEmail("test@test.com");
        msg.setTransactionUuid("uuid-" + lineNumber);
        msg.setDeclaredTotalRecords(10);
        msg.setOriginatingAccount("0001111111");
        return msg;
    }
}
