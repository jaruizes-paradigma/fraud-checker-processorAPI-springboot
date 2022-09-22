package com.paradigma.rt.streaming.processorapi.fraudchecker.processors;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class OnlineMovementsAggregatorProcessor extends MovementsAggregatorProcessor {

    public OnlineMovementsAggregatorProcessor(int onlineSessionInactivyGap, int onlineCheckInactiveSessionsInterval) {
        super(onlineSessionInactivyGap, onlineCheckInactiveSessionsInterval);
        this.stateStoreName = "online-aggregator-store";
    }
}