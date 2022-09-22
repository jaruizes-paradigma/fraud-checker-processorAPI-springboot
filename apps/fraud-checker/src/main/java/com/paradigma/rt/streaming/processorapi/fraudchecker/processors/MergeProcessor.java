package com.paradigma.rt.streaming.processorapi.fraudchecker.processors;

import com.paradigma.rt.streaming.processorapi.fraudchecker.FraudCheckerUtils;
import com.paradigma.rt.streaming.processorapi.fraudchecker.model.FraudCase;
import com.paradigma.rt.streaming.processorapi.fraudchecker.model.MovementsAggregation;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class MergeProcessor implements Processor<String, MovementsAggregation, String, FraudCase> {

    private ProcessorContext<String,FraudCase> context;

    @Override
    public void init(ProcessorContext<String, FraudCase> context) {
        this.context = context;
    }

    @Override
    public void process(Record<String, MovementsAggregation> record) {
        final FraudCase fraudCase = FraudCheckerUtils.movementsAggregationToFraudCase(record.value());
        context.forward(new Record<>(fraudCase.getCard(), fraudCase, record.timestamp()));
        context.commit();
    }

    @Override
    public void close() { }

}
