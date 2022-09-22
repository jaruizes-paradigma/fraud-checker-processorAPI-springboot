package com.paradigma.rt.streaming.processorapi.fraudchecker.processors;

import com.paradigma.rt.streaming.processorapi.fraudchecker.model.Movement;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class MapProcessor implements Processor<String, Movement, String, Movement> {

    private ProcessorContext<String,Movement> context;

    @Override
    public void init(ProcessorContext<String, Movement> context) {
        this.context = context;
    }

    @Override
    public void process(Record<String, Movement> record) {
        context.forward(new Record<>(record.value().getCard(), record.value(), record.timestamp()));
    }

    @Override
    public void close() {}

}
