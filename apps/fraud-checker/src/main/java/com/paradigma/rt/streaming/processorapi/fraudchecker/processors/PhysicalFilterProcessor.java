package com.paradigma.rt.streaming.processorapi.fraudchecker.processors;

import com.paradigma.rt.streaming.processorapi.fraudchecker.model.MovementsAggregation;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import java.util.HashSet;
import java.util.Set;

public class PhysicalFilterProcessor implements Processor<String, MovementsAggregation, String, MovementsAggregation> {
    
    public final static int MULTIPLE_PHYSICAL_MOVEMENTS_IN_SHORT_PERIOD = 4;
    public final static int ALLOWED_PHYSICAL_DEVICES_IN_SHORT_PERIOD = 1;

    private ProcessorContext<String, MovementsAggregation> context;

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
        final Set<String> devices = new HashSet<>();
        movementsAggregation.getMovements().forEach((movement -> devices.add(movement.getDevice())));

        // Multiple devices during the session
        return devices.size() > ALLOWED_PHYSICAL_DEVICES_IN_SHORT_PERIOD || movementsAggregation.getMovements().size() > MULTIPLE_PHYSICAL_MOVEMENTS_IN_SHORT_PERIOD;
    }

}
