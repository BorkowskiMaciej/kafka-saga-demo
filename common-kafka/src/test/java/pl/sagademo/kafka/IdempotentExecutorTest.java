package pl.sagademo.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import pl.sagademo.events.EventEnvelope;
import pl.sagademo.events.EventTypes;
import pl.sagademo.events.payload.StockReserved;

class IdempotentExecutorTest {

    private final JdbcClient jdbcClient = mock(JdbcClient.class, RETURNS_DEEP_STUBS);
    private final PlatformTransactionManager transactionManager = mock(PlatformTransactionManager.class);
    private final IdempotentExecutor executor =
            new IdempotentExecutor(jdbcClient, new TransactionTemplate(transactionManager));

    private final UUID orderId = UUID.randomUUID();
    private final EventEnvelope<StockReserved> event =
            EventEnvelope.of(EventTypes.STOCK_RESERVED, orderId, new StockReserved(orderId));

    @Test
    void runsActionForNewEvent() {
        stubInsertResult(1);
        AtomicInteger executions = new AtomicInteger();

        boolean executed = executor.runOnce(event, executions::incrementAndGet);

        assertThat(executed).isTrue();
        assertThat(executions).hasValue(1);
    }

    @Test
    void skipsActionForDuplicateEvent() {
        stubInsertResult(0);
        AtomicInteger executions = new AtomicInteger();

        boolean executed = executor.runOnce(event, executions::incrementAndGet);

        assertThat(executed).isFalse();
        assertThat(executions).hasValue(0);
    }

    @Test
    void rollsBackAndPropagatesActionFailure() {
        stubInsertResult(1);

        assertThatThrownBy(() -> executor.runOnce(event, () -> {
            throw new IllegalStateException("boom");
        })).isInstanceOf(IllegalStateException.class);

        verify(transactionManager).rollback(any());
    }

    private void stubInsertResult(int rows) {
        when(jdbcClient.sql(anyString()).param(anyString(), any()).param(anyString(), any()).update())
                .thenReturn(rows);
    }
}
