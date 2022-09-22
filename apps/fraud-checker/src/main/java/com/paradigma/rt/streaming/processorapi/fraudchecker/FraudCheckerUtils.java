package com.paradigma.rt.streaming.processorapi.fraudchecker;

import com.paradigma.rt.streaming.processorapi.fraudchecker.model.FraudCase;
import com.paradigma.rt.streaming.processorapi.fraudchecker.model.MovementsAggregation;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class FraudCheckerUtils {

    public static long iso8601ToEpoch(String createdAt) {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        try {
            return formatter.parse(createdAt).getTime();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public static FraudCase movementsAggregationToFraudCase(MovementsAggregation movementsAggregation) {
        final FraudCase fraudCase = new FraudCase();
        movementsAggregation.getMovements().forEach((movement -> fraudCase.addMovement(movement)));

        return fraudCase;
    }

}