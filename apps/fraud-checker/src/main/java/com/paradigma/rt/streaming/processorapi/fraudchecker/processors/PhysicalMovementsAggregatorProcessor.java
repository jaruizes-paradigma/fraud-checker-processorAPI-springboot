package com.paradigma.rt.streaming.processorapi.fraudchecker.processors;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class PhysicalMovementsAggregatorProcessor extends MovementsAggregatorProcessor {

    public PhysicalMovementsAggregatorProcessor(int onlineSessionInactivyGap, int onlineCheckInactiveSessionsInterval) {
        super(onlineSessionInactivyGap, onlineCheckInactiveSessionsInterval);
        this.stateStoreName = "physical-aggregator-store";
    }
}