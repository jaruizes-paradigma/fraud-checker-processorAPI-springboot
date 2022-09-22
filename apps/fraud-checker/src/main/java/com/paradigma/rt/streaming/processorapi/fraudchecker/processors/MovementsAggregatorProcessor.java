package com.paradigma.rt.streaming.processorapi.fraudchecker.processors;

import com.paradigma.rt.streaming.processorapi.fraudchecker.FraudCheckerUtils;
import com.paradigma.rt.streaming.processorapi.fraudchecker.model.Movement;
import com.paradigma.rt.streaming.processorapi.fraudchecker.model.MovementsAggregation;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.time.Instant;


@Log4j2
public class MovementsAggregatorProcessor implements Processor<String, Movement, String, MovementsAggregation> {
    
    private ProcessorContext<String, MovementsAggregation> context;
    private KeyValueStore<String, MovementsAggregation> kvStore;
    private Cancellable punctuator;
    protected String stateStoreName;
    private int checkInactiveSessionsInterval;
    private int sessionInactivityGap;

    public MovementsAggregatorProcessor(int sessionInactivyGap, int checkInactiveSessionsInterval) {
        this.checkInactiveSessionsInterval = checkInactiveSessionsInterval;
        this.sessionInactivityGap = sessionInactivyGap;
    }

    @Override
    public void init(ProcessorContext<String, MovementsAggregation> context) {
        this.context = context;
        this.kvStore = (KeyValueStore) context.getStateStore(stateStoreName);
        
        // clean key-value store
        try (KeyValueIterator<String, MovementsAggregation> iterator = this.kvStore.all()) {
            iterator.forEachRemaining(entry -> {
                this.kvStore.delete(entry.key);
            });
        }

        punctuator = this.context.schedule(Duration.ofSeconds(this.checkInactiveSessionsInterval), PunctuationType.WALL_CLOCK_TIME, this::closeInactiveSessions);
    }
    
    @Override
    public void process(Record<String, Movement> currentOnlineMovement) {
        String key = currentOnlineMovement.key();

        log.info("Processing incoming movement with id <{}> and key <{}>", currentOnlineMovement.value().getId(), key);

        MovementsAggregation lastMovementStored = kvStore.get(key);

        if (kvStore.get(key) == null) {
            openNewSessionWindowWithTheNewMovement(currentOnlineMovement);
        } else {
            if (isSessionEndedForTheKey(key, currentOnlineMovement.timestamp())) {
                log.info("Detected session expired for movements associated to card <{}>. Closing session and emitting result", key);
                closeSessionAndEmitResult(key, lastMovementStored);
                openNewSessionWindowWithTheNewMovement(currentOnlineMovement);
            } else {
                updateAggregationStored(currentOnlineMovement, lastMovementStored);
            }
        }
    }
    
    @Override
    public void close() {
        punctuator.cancel();
        closeInactiveSessions(Instant.now().toEpochMilli());
        try (KeyValueIterator<String, MovementsAggregation> iterator = this.kvStore.all()) {
            iterator.forEachRemaining(entry -> {
                this.kvStore.delete(entry.key);
            });
        }
    }

    private void closeInactiveSessions(Long currentTimestamp) {
        log.info("[{}] Scheduled action: looking for inactive sessions....", currentTimestamp);
        try (KeyValueIterator<String, MovementsAggregation> iterator = this.kvStore.all()) {
            while(iterator.hasNext()) {
                KeyValue<String, MovementsAggregation> entry = iterator.next();
                if (isSessionEndedForTheKey(entry.key, currentTimestamp)) {
                    log.info("\t -> Inactive session found for card: <{}>. Closing session and emitting result", entry.key);
                    closeSessionAndEmitResult(entry.key, entry.value);
                }
            }
        }
        log.info("[{}] Scheduled action: looking for inactive sessions finished", currentTimestamp);
    }
    
    private void updateAggregationStored(Record<String, Movement> onlineRecord, MovementsAggregation lastAggregationStored) {
        lastAggregationStored.addMovement(onlineRecord.value());
        kvStore.put(onlineRecord.key(), lastAggregationStored);
    }

    private boolean isSessionEndedForTheKey(String key, long referenceTimestamp) {
        MovementsAggregation aggregationStored = this.kvStore.get(key);
        if (aggregationStored != null) {
            long lastTimestamp = FraudCheckerUtils.iso8601ToEpoch(aggregationStored.getLastMovementTimestamp());
            return (referenceTimestamp - lastTimestamp) / 1000 > sessionInactivityGap;
        }

        return false;
    }

    private void closeSessionAndEmitResult(String key, MovementsAggregation lastMovementAggregationStored) {
        // Clean key from storage
        this.kvStore.delete(key);

        // Forward fraud
        context.forward(new Record<>(key, lastMovementAggregationStored, Instant.now().toEpochMilli()));
    }

    private MovementsAggregation openNewSessionWindowWithTheNewMovement(Record<String, Movement> onlineRecord) {
        MovementsAggregation newMovementsAgreggation = new MovementsAggregation();
        newMovementsAgreggation.addMovement(onlineRecord.value());

        kvStore.put(onlineRecord.key(), newMovementsAgreggation);

        return newMovementsAgreggation;
    }
    
}