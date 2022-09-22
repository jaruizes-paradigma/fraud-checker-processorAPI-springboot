package com.paradigma.rt.streaming.processorapi.fraudchecker.processors;

import com.paradigma.rt.streaming.processorapi.fraudchecker.model.Movement;
import com.paradigma.rt.streaming.processorapi.fraudchecker.model.MovementsAggregation;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import java.util.Set;

public class OnlineFilterProcessor implements Processor<String, MovementsAggregation, String, MovementsAggregation> {

    public final static float ALLOWED_ONLINE_AMOUNT_IN_SHORT_PERIOD = 200;
    public final static int MULTIPLE_ONLINE_MOVEMENTS_IN_SHORT_PERIOD = 3;

    private ProcessorContext<String, MovementsAggregation> context;

    public OnlineFilterProcessor() {}

    @Override
    public void init(ProcessorContext<String, MovementsAggregation> context) {
        this.context = context;
     }

    @Override
    public void process(Record<String, MovementsAggregation> record) {
        MovementsAggregation movementsAggregation = record.value();
        if (isFraud(movementsAggregation)){
            context.forward(record);
        }
    }

    @Override
    public void close() {}

    private boolean isFraud(MovementsAggregation movementsAggregation) {
        if (movementsAggregation.getMovements().size() > MULTIPLE_ONLINE_MOVEMENTS_IN_SHORT_PERIOD) {
            final Set<Movement> movements = movementsAggregation.getMovements();
            double totalAmount = movements.stream().mapToDouble((movement) -> movement.getAmount()).sum();

            return totalAmount > ALLOWED_ONLINE_AMOUNT_IN_SHORT_PERIOD;
        }

        return false;
    }
}
