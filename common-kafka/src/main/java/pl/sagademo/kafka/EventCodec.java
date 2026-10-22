package pl.sagademo.kafka;

import org.springframework.core.ResolvableType;
import pl.sagademo.events.EventEnvelope;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.json.JsonMapper;

public class EventCodec {

    private final JsonMapper jsonMapper;

    public EventCodec(JsonMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }

    public String write(EventEnvelope<?> envelope) {
        return jsonMapper.writeValueAsString(envelope);
    }

    public <T> EventEnvelope<T> read(String json, Class<T> payloadType) {
        JavaType type = jsonMapper.constructType(
                ResolvableType.forClassWithGenerics(EventEnvelope.class, payloadType).getType());
        return jsonMapper.readValue(json, type);
    }
}
