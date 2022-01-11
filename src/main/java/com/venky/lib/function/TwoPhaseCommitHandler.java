package com.venky.lib.function;

import com.google.protobuf.GeneratedMessageV3;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.venky.lib.function.SynchronousCommitHandler.IS_KAFKA_EXCEPTION;

@Component
public class TwoPhaseCommitHandler<E, X, P extends GeneratedMessageV3> {

    private final static Logger LOGGER = LogManager.getLogger(TwoPhaseCommitHandler.class.getName());
    SynchronousCommitHandler<E, X, P> synchronousCommitHandler = null;
    @Autowired
    private ObjectFactory<SynchronousCommitHandler<E, X, P>> synchronousCommitHandlerFactory;
    private Supplier<E> persistenceSupplier;
    private X key;
    private Function<E, P> mapper;
    private String topicName;
    private Supplier<E> correctionRecord;

    public TwoPhaseCommitHandler<E, X, P> init() {
        synchronousCommitHandler = synchronousCommitHandlerFactory.getObject();
        return this;
    }

    public TwoPhaseCommitHandler<E, X, P> setCorrectionRecord(Supplier<E> correctionRecord) {
        synchronousCommitHandler.setCorrectionRecord(correctionRecord);
        return this;
    }

    public TwoPhaseCommitHandler<E, X, P> persistWith(Supplier<E> supplier) {
        synchronousCommitHandler.persistWith(supplier);
        return this;
    }

    public TwoPhaseCommitHandler<E, X, P> topic(String topicName) {
        synchronousCommitHandler.topic(topicName);
        return this;
    }

    public TwoPhaseCommitHandler<E, X, P> key(X key) {
        synchronousCommitHandler.key(key);
        return this;
    }

    public TwoPhaseCommitHandler<E, X, P> valueMapper(Function<E, P> mapper) {
        synchronousCommitHandler.value(mapper);
        return this;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, noRollbackFor = Throwable.class)
    public void execute() {
        try {
            synchronousCommitHandler.execute();
        } catch (Exception ex) {
            handleException(ex);
            throw ex;
        }
    }

    @Transactional(propagation = Propagation.NESTED, isolation = Isolation.REPEATABLE_READ)
    protected void handleException(Exception ex) {

        try {
            LOGGER.error("rolling back transaction due to : {}", ex.getMessage(), ex);
            if (Predicate.not(IS_KAFKA_EXCEPTION).test(ex) && Predicate.not(IS_KAFKA_EXCEPTION).test(ex.getCause())) {
                LOGGER.error("sending correction record to kafka: {}", correctionRecord.get());
                synchronousCommitHandler.sendCorrection = true;
            }
        } finally {
            if (synchronousCommitHandler.isKafkaSend.get() && synchronousCommitHandler.sendCorrection) {
                synchronousCommitHandler.sendCorrection();
            }

        }
    }
}

