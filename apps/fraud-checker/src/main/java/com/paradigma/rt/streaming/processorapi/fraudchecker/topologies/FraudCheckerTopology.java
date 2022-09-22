package com.paradigma.rt.streaming.processorapi.fraudchecker.topologies;


import com.paradigma.rt.streaming.processorapi.fraudchecker.config.FraudCheckerConfig;
import com.paradigma.rt.streaming.processorapi.fraudchecker.extractors.MovementTimestampExtractor;
import com.paradigma.rt.streaming.processorapi.fraudchecker.model.FraudCase;
import com.paradigma.rt.streaming.processorapi.fraudchecker.model.Movement;
import com.paradigma.rt.streaming.processorapi.fraudchecker.model.MovementsAggregation;
import com.paradigma.rt.streaming.processorapi.fraudchecker.processors.*;
import com.paradigma.rt.streaming.processorapi.fraudchecker.serializers.JsonDeserializer;
import com.paradigma.rt.streaming.processorapi.fraudchecker.serializers.JsonSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class FraudCheckerTopology {

    private FraudCheckerConfig config;

    Serde<Movement> movementSerde = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(Movement.class));
    Serde<FraudCase> fraudSerde = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(FraudCase.class));
    Serde<MovementsAggregation> movementsAggregationSerde = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(MovementsAggregation.class));

    @Autowired
    public FraudCheckerTopology(FraudCheckerConfig config) {
        this.config = config;
    }

    @Autowired
    public void buildTopology(StreamsBuilder streamsBuilder) {
        
        Topology topology = streamsBuilder.build();

        // source (card movements)
        topology.addSource(Topology.AutoOffsetReset.EARLIEST,
                "Movement Source",
                new MovementTimestampExtractor(),
                Serdes.String().deserializer(),
                movementSerde.deserializer(),
                config.getMovementsTopic());

        // stateless (generates new key and splits movements by origin)
        topology.addProcessor("Map", MapProcessor::new,"Movement Source");

        // Split: physical and online movements processed independently
        topology.addProcessor("Split", SplitProcessor::new,"Map");

        // ONLINE
        StoreBuilder<KeyValueStore<String, MovementsAggregation>> onlineFraudStoreBuilder = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("online-aggregator-store"),
                Serdes.String(), movementsAggregationSerde);

        // Aggregation of movements during a session
        topology.addProcessor("Online movements aggregator", this::initializeOnlineAggregation,"Split")
                .addStateStore(onlineFraudStoreBuilder,"Online movements aggregator");

        // Filter: detect online fraud cases
        topology.addProcessor("Online fraud cases filter", OnlineFilterProcessor::new, "Online movements aggregator");

        // PHYSICAL
        StoreBuilder<KeyValueStore<String, MovementsAggregation>> physicalFraudStoreBuilder = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("physical-aggregator-store"),
                Serdes.String(), movementsAggregationSerde);

        // Aggregation of movements during a session
        topology.addProcessor("Physical movements aggregator",this::initializePhysicalAggregation,"Split")
                .addStateStore(physicalFraudStoreBuilder, "Physical movements aggregator");

        // Filter: detect online fraud cases
        topology.addProcessor("Physical fraud cases filter", PhysicalFilterProcessor::new, "Physical movements aggregator");

        // Merge and map
        topology.addProcessor("Merge fraud cases", MergeProcessor::new,"Online fraud cases filter", "Physical fraud cases filter");

        topology.addSink("Fraud cases sink",
                config.getFraudTopic(),
                Serdes.String().serializer(),
                fraudSerde.serializer(),
                "Merge fraud cases");
    }

    private OnlineMovementsAggregatorProcessor initializeOnlineAggregation() {
        return new OnlineMovementsAggregatorProcessor(config.getSessionInactivityGap(), config.getCheckInactiveSessionsInterval());
    }

    private PhysicalMovementsAggregatorProcessor initializePhysicalAggregation() {
        return new PhysicalMovementsAggregatorProcessor(config.getSessionInactivityGap(), config.getCheckInactiveSessionsInterval());
    }

}
