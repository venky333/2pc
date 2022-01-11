package com.venky.lib.function;

import com.google.protobuf.GeneratedMessageV3;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.interceptor.TransactionInterceptor;
import org.springframework.util.Assert;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

@Component
@Scope(value = "prototype")
public class SynchronousCommitHandler<E, X, P extends GeneratedMessageV3> {

    protected static final Predicate<Throwable> IS_KAFKA_EXCEPTION = t -> (t instanceof KafkaException) || (t instanceof TimeoutException);
    private final static Logger LOGGER = LogManager.getLogger(SynchronousCommitHandler.class.getName());
    protected final AtomicBoolean isKafkaSend = new AtomicBoolean(false);
    protected boolean sendCorrection = false;
    @Autowired
    KafkaTemplate<X, byte[]> kafkaTemplate;
    private boolean canExecute = false;
    private Supplier<E> persistenceSupplier;
    private X key;
    private Function<E, P> mapper;
    private String topicName;
    private Supplier<E> correctionRecord;


    protected void setCorrectionRecord(Supplier<E> correctionRecord) {
        this.correctionRecord = correctionRecord;
    }

    protected void persistWith(Supplier<E> supplier) {
        this.persistenceSupplier = supplier;
    }

    protected void topic(String topicName) {
        this.topicName = topicName;
    }

    protected void key(X key) {
        this.key = key;
    }

    protected void value(Function<E, P> mapper) {
        this.mapper = mapper;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, isolation = Isolation.REPEATABLE_READ)
    protected void execute() {

        E result = persistenceSupplier.get();
        try {
            Optional.of(kafkaTemplate)
                    .map(val -> val.send(this.topicName, this.key, mapper.apply(result).toByteArray()))
                    .map(val -> {
                        try {
                            return val.get(1, TimeUnit.SECONDS);
                        } catch (Exception e) {
                            LOGGER.error("error producing record to kafka: {}", result);
                            throw new RuntimeException(e);
                        }
                    })
                    .ifPresent(val -> isKafkaSend.set(true));
        } catch (Exception ex) {
            LOGGER.error("rolling back transaction due to {}", ex.getMessage(), ex);
            TransactionInterceptor.currentTransactionStatus().setRollbackOnly();
            throw ex;
        }

    }

    protected void sendCorrection() {
        validateFields();
        kafkaTemplate.send(this.topicName, this.key, mapper.apply(correctionRecord.get()).toByteArray());
    }

    private void validateFields() {
        Assert.notNull(this.topicName, () -> "topic name has not been initialised");
        Assert.notNull(this.key, () -> "record key has not been initialised");
        Assert.notNull(this.mapper, () -> "mapper key has not been initialised");
        Assert.notNull(this.correctionRecord, () -> "correction record supplier has not been initialised");
        Assert.notNull(this.persistenceSupplier, () -> "persistence supplier key has not been initialised");
    }
}
