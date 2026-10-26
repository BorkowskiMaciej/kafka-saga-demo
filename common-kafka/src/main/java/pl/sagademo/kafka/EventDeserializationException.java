package pl.sagademo.kafka;

public class EventDeserializationException extends RuntimeException {

    public EventDeserializationException(Class<?> payloadType, Throwable cause) {
        super("Cannot deserialize event payload " + payloadType.getSimpleName(), cause);
    }
}
