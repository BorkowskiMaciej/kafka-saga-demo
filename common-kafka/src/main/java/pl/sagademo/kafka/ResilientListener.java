package pl.sagademo.kafka;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.core.annotation.AliasFor;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@RetryableTopic(
        kafkaTemplate = "kafkaTemplate",
        attempts = "4",
        backOff = @BackOff(delay = 1000, multiplier = 2, maxDelay = 8000),
        numPartitions = "3",
        replicationFactor = "1",
        topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
        exclude = EventDeserializationException.class,
        traversingCauses = "true")
public @interface ResilientListener {

    @AliasFor(annotation = RetryableTopic.class, attribute = "retryTopicSuffix")
    String retryTopicSuffix();

    @AliasFor(annotation = RetryableTopic.class, attribute = "dltTopicSuffix")
    String dltTopicSuffix();
}
