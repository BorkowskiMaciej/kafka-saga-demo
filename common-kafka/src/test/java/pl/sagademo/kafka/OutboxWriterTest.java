package pl.sagademo.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionSynchronizationUtils;
import pl.sagademo.events.EventEnvelope;
import pl.sagademo.events.EventTypes;
import pl.sagademo.events.payload.StockReserved;

class OutboxWriterTest {

    private final JdbcClient jdbcClient = mock(JdbcClient.class, RETURNS_DEEP_STUBS);
    private final EventCodec codec = mock(EventCodec.class);
    private final AtomicInteger wakeUps = new AtomicInteger();
    private final OutboxWriter writer = new OutboxWriter(jdbcClient, codec, wakeUps::incrementAndGet);

    private final UUID orderId = UUID.randomUUID();
    private final EventEnvelope<StockReserved> event =
            EventEnvelope.of(EventTypes.STOCK_RESERVED, orderId, new StockReserved(orderId));

    @AfterEach
    void clearTransactionState() {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.clearSynchronization();
        }
        TransactionSynchronizationManager.setActualTransactionActive(false);
    }

    @Test
    void refusesToEnqueueOutsideTransaction() {
        assertThatThrownBy(() -> writer.enqueue("inventory.reserved", event, "corr-1"))
                .isInstanceOf(IllegalStateException.class);

        verifyNoInteractions(jdbcClient);
        assertThat(wakeUps).hasValue(0);
    }

    @Test
    void storesRowAndWakesRelayOnlyAfterCommit() {
        when(codec.write(any())).thenReturn("{\"json\":true}");
        beginTransaction();

        writer.enqueue("inventory.reserved", event, "corr-1");

        verify(jdbcClient).sql(anyString());
        assertThat(wakeUps).hasValue(0);

        TransactionSynchronizationUtils.invokeAfterCommit(TransactionSynchronizationManager.getSynchronizations());

        assertThat(wakeUps).hasValue(1);
    }

    @Test
    void doesNotWakeRelayWhenTransactionRollsBack() {
        when(codec.write(any())).thenReturn("{\"json\":true}");
        beginTransaction();

        writer.enqueue("inventory.reserved", event, "corr-1");
        TransactionSynchronizationUtils.invokeAfterCompletion(
                TransactionSynchronizationManager.getSynchronizations(), TransactionSynchronization.STATUS_ROLLED_BACK);

        assertThat(wakeUps).hasValue(0);
    }

    private static void beginTransaction() {
        TransactionSynchronizationManager.initSynchronization();
        TransactionSynchronizationManager.setActualTransactionActive(true);
    }
}
