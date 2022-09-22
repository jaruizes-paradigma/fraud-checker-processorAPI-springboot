package com.paradigma.rt.streaming.processorapi.fraudchecker.processors;

import com.paradigma.rt.streaming.processorapi.fraudchecker.model.Movement;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class SplitProcessor implements Processor<String, Movement, String, Movement> {

    private static final int ONLINE_MOVEMENT = 3;
    private ProcessorContext<String,Movement> context;

    @Override
    public void init(ProcessorContext<String, Movement> context) {
        this.context = context;
    }

    @Override
    public void process(Record<String, Movement> record) {
        if (record.value().getOrigin() == ONLINE_MOVEMENT){
            context.forward(record, "Online movements aggregator");
        } else {
            context.forward(record, "Physical movements aggregator");
        }
    }

    @Override
    public void close() {}

}
