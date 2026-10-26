package pl.sagademo.kafka;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import pl.sagademo.events.payload.OrderCreated;
import tools.jackson.databind.json.JsonMapper;

class EventCodecFailureTest {

    private final EventCodec codec = new EventCodec(JsonMapper.builder().build());

    @Test
    void wrapsMalformedJsonInDeserializationException() {
        assertThatThrownBy(() -> codec.read("{ not json", OrderCreated.class))
                .isInstanceOf(EventDeserializationException.class);
    }

    @Test
    void wrapsInvalidFieldValuesInDeserializationException() {
        assertThatThrownBy(() -> codec.read("{\"eventId\":\"not-a-uuid\"}", OrderCreated.class))
                .isInstanceOf(EventDeserializationException.class);
    }
}
