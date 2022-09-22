package com.paradigma.rt.streaming.processorapi.fraudchecker.testdriver;

import com.paradigma.rt.streaming.processorapi.fraudchecker.config.FraudCheckerConfig;
import com.paradigma.rt.streaming.processorapi.fraudchecker.model.FraudCase;
import com.paradigma.rt.streaming.processorapi.fraudchecker.model.Movement;
import com.paradigma.rt.streaming.processorapi.fraudchecker.serializers.JsonDeserializer;
import com.paradigma.rt.streaming.processorapi.fraudchecker.serializers.JsonSerializer;
import com.paradigma.rt.streaming.processorapi.fraudchecker.topologies.FraudCheckerTopology;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import static com.paradigma.rt.streaming.processorapi.fraudchecker.config.FraudCheckerConfig.ONLINE_FRAUD_DESCRIPTION;
import static com.paradigma.rt.streaming.processorapi.fraudchecker.config.FraudCheckerConfig.PHYSICAL_FRAUD_DESCRIPTION;

public class FraudCheckerTests {

    public static final String MOVEMENTS_TOPIC = "movements";
    public static final String FRAUD_TOPIC = "fraud-cases";
    public static final int ATM_MOVEMENT = 1;
    public static final int MERCHANT_MOVEMENT = 2;
    public static final int ONLINE_MOVEMENT = 3;

    private FraudCheckerTopology fraudCheckerProcessor;
    private Topology topology;
    private DateTimeFormatter formatter;
    private final Serde<Movement> movementSerde = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(Movement.class));
    private final Serde<FraudCase> fraudSerde = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(FraudCase.class));

    @BeforeEach
    void setUp() {
        FraudCheckerConfig config = new FraudCheckerConfig("movements", "fraud-cases", 60, 10);
        fraudCheckerProcessor = new FraudCheckerTopology(config);
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        fraudCheckerProcessor.buildTopology(streamsBuilder);
        topology = streamsBuilder.build();
        formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z").withLocale(Locale.UK).withZone(ZoneId.systemDefault());
    }

    @Test
    void shouldNotDetectFraudCases() {
        Instant instant = Instant.now();
        try (TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, new Properties(), Instant.now())) {

            TestInputTopic<String, Movement> inputTopic = topologyTestDriver.createInputTopic(MOVEMENTS_TOPIC, new StringSerializer(), movementSerde.serializer());
            TestOutputTopic<String, FraudCase> outputTopic = topologyTestDriver.createOutputTopic(FRAUD_TOPIC, new StringDeserializer(), fraudSerde.deserializer());

            List<Movement> inputMovements = Arrays.asList(
                    // init window
                    Movement.builder().id("m0").amount(10f).device("atm-1").site("site0").origin(ATM_MOVEMENT).card("c1").createdAt(formatter.format(instant)).build(),
                    // fraud (c2: MULTIPLE_ONLINE_MOVEMENTS_IN_SHORT_PERIOD && ALLOWED_ONLINE_AMOUNT_IN_SHORT_PERIOD)
                    Movement.builder().id("m1").amount(10f).device("atm-1").site("site1").origin(ATM_MOVEMENT).card("c1").createdAt(formatter.format(instant.plusSeconds(15))).build(),
                    Movement.builder().id("m2").amount(10f).device("atm-1").site("site2").origin(MERCHANT_MOVEMENT).card("c1").createdAt(formatter.format(instant.plusSeconds(75))).build(),
                    Movement.builder().id("m3").amount(10f).device("atm-1").site("site1").origin(ONLINE_MOVEMENT).card("c1").createdAt(formatter.format(instant.plusSeconds(95))).build(),
                    Movement.builder().id("m4").amount(10f).device("atm-2").site("site3").origin(ATM_MOVEMENT).card("c2").createdAt(formatter.format(instant.plusSeconds(5))).build(),
                    Movement.builder().id("m5").amount(10f).device("atm-1").site("site3").origin(MERCHANT_MOVEMENT).card("c2").createdAt(formatter.format(instant.plusSeconds(66))).build(),
                    // close window
                    Movement.builder().id("m6").amount(10f).device("atm-1").site("site0").origin(ATM_MOVEMENT).card("c5").createdAt(formatter.format(instant.plusSeconds(500))).build()
            );
            inputTopic.pipeValueList(inputMovements);

            // assertions
            List<FraudCase> fraudCases = outputTopic.readValuesToList();
            Assertions.assertEquals(0, fraudCases.size());

        }

    }

    @Test
    void shouldDetectOnlineFraudCaseBecauseMultipleMovements() throws InterruptedException {
        Instant instant = Instant.now();
        try (TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, new Properties(), Instant.now())) {

            TestInputTopic<String, Movement> inputTopic = topologyTestDriver.createInputTopic(MOVEMENTS_TOPIC, new StringSerializer(), movementSerde.serializer());
            TestOutputTopic<String, FraudCase> outputTopic = topologyTestDriver.createOutputTopic(FRAUD_TOPIC, new StringDeserializer(), fraudSerde.deserializer());

            List<Movement> inputMovements = Arrays.asList(
                    // init window
                    Movement.builder().id("m0").amount(1000f).device("").site("site0").origin(ONLINE_MOVEMENT).card("c1").createdAt(formatter.format(instant)).build(),
                    // fraud (c2: MULTIPLE_ONLINE_MOVEMENTS_IN_SHORT_PERIOD && ALLOWED_ONLINE_AMOUNT_IN_SHORT_PERIOD)
                    Movement.builder().id("m1").amount(10f).device("").site("site1").origin(ONLINE_MOVEMENT).card("c2").createdAt(formatter.format(instant.plusSeconds(10))).build(),
                    Movement.builder().id("m2").amount(90f).device("").site("site2").origin(ONLINE_MOVEMENT).card("c2").createdAt(formatter.format(instant.plusSeconds(15))).build(),
                    Movement.builder().id("m3").amount(50f).device("").site("site2").origin(ONLINE_MOVEMENT).card("c3").createdAt(formatter.format(instant.plusSeconds(20))).build(),
                    Movement.builder().id("m4").amount(200f).device("").site("site3").origin(ONLINE_MOVEMENT).card("c2").createdAt(formatter.format(instant.plusSeconds(30))).build(),
                    Movement.builder().id("m5").amount(90f).device("").site("site3").origin(ONLINE_MOVEMENT).card("c3").createdAt(formatter.format(instant.plusSeconds(40))).build(),
                    Movement.builder().id("m6").amount(100f).device("").site("site4").origin(ONLINE_MOVEMENT).card("c2").createdAt(formatter.format(instant.plusSeconds(44))).build()
            );
            inputTopic.pipeValueList(inputMovements);

            // Last record + INACTIVITY GAP
            topologyTestDriver.advanceWallClockTime(Duration.ofSeconds(105));

            // assertions
            List<FraudCase> fraudCases = outputTopic.readValuesToList();
            Assertions.assertEquals(1, fraudCases.size());
            Assertions.assertEquals(4, fraudCases.get(0).getMovements().size());
            Assertions.assertEquals(400L, fraudCases.get(0).getTotalAmount());
            Assertions.assertEquals(ONLINE_FRAUD_DESCRIPTION, fraudCases.get(0).getDescription());
            Assertions.assertEquals("c2", fraudCases.get(0).getCard());

        }

    }

    @Test
    void shouldDetectTwoPhysicalFraudCasesBecauseOperationsInMultipleDevicesDuringAShortPeriod() {
        Instant instant = Instant.now();
        try (TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, new Properties())) {

            TestInputTopic<String, Movement> inputTopic = topologyTestDriver.createInputTopic(MOVEMENTS_TOPIC, new StringSerializer(), movementSerde.serializer());
            TestOutputTopic<String, FraudCase> outputTopic = topologyTestDriver.createOutputTopic(FRAUD_TOPIC, new StringDeserializer(), fraudSerde.deserializer());

            List<Movement> physicalFraudMovements = Arrays.asList(
                    // init window
                    Movement.builder().id("m0").amount(1000f).device("atm-0").site("site0").origin(ATM_MOVEMENT).card("c1").createdAt(formatter.format(instant)).build(),
                    // fraud (c2,c3: ALLOWED_PHYSICAL_DEVICES_IN_SHORT_PERIOD)
                    Movement.builder().id("m1").amount(10f).device("atm-1").site("site1").origin(ATM_MOVEMENT).card("c2").createdAt(formatter.format(instant.plusSeconds(5))).build(),
                    Movement.builder().id("m2").amount(90f).device("atm-2").site("site2").origin(ATM_MOVEMENT).card("c2").createdAt(formatter.format(instant.plusSeconds(15))).build(),
                    Movement.builder().id("m3").amount(90f).device("atm-2").site("site2").origin(ATM_MOVEMENT).card("c3").createdAt(formatter.format(instant.plusSeconds(20))).build(),
                    Movement.builder().id("m4").amount(100f).device("shop-1").site("site3").origin(MERCHANT_MOVEMENT).card("c2").createdAt(formatter.format(instant.plusSeconds(30))).build(),
                    Movement.builder().id("m5").amount(90f).device("atm-1").site("site1").origin(ATM_MOVEMENT).card("c3").createdAt(formatter.format(instant.plusSeconds(40))).build()
                    // close window
                    //Movement.builder().id("m6").amount(1000f).device("shop-4").site("site4").origin(MERCHANT_MOVEMENT).card("c4").createdAt(formatter.format(instant.plusSeconds(545))).build()
            );
            inputTopic.pipeValueList(physicalFraudMovements);

            // Last record + INACTIVITY GAP
            topologyTestDriver.advanceWallClockTime(Duration.ofSeconds(105));

            // assertions
            List<FraudCase> fraudCases = outputTopic.readValuesToList();
            Assertions.assertEquals(2, fraudCases.size());

        }

    }

    @Test
    void shouldDetectOnePhysicalFraudCaseBecauseMoreThanFourMovementsInAShortPeriod() {
        Instant instant = Instant.now();
        try (TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, new Properties())) {

            TestInputTopic<String, Movement> inputTopic = topologyTestDriver.createInputTopic(MOVEMENTS_TOPIC, new StringSerializer(), movementSerde.serializer());
            TestOutputTopic<String, FraudCase> outputTopic = topologyTestDriver.createOutputTopic(FRAUD_TOPIC, new StringDeserializer(), fraudSerde.deserializer());

            List<Movement> physicalFraudMovements = Arrays.asList(
                    // init window
                    Movement.builder().id("m0").amount(1000f).device("atm-0").site("site0").origin(ATM_MOVEMENT).card("c0").createdAt(formatter.format(instant)).build(),
                    // fraud (c1: MULTIPLE_PHYSICAL_MOVEMENTS_IN_SHORT_PERIOD)
                    Movement.builder().id("m1").amount(10f).device("atm-2").site("site2").origin(ATM_MOVEMENT).card("c1").createdAt(formatter.format(instant.plusSeconds(5))).build(),
                    Movement.builder().id("m2").amount(90f).device("atm-2").site("site2").origin(ATM_MOVEMENT).card("c1").createdAt(formatter.format(instant.plusSeconds(15))).build(),
                    Movement.builder().id("m3").amount(90f).device("atm-2").site("site2").origin(ATM_MOVEMENT).card("c1").createdAt(formatter.format(instant.plusSeconds(20))).build(),
                    Movement.builder().id("m4").amount(100f).device("atm-2").site("site2").origin(ATM_MOVEMENT).card("c1").createdAt(formatter.format(instant.plusSeconds(30))).build(),
                    Movement.builder().id("m5").amount(90f).device("atm-2").site("site2").origin(ATM_MOVEMENT).card("c1").createdAt(formatter.format(instant.plusSeconds(40))).build()

            );
            inputTopic.pipeValueList(physicalFraudMovements);
            // Last record + INACTIVITY GAP
            topologyTestDriver.advanceWallClockTime(Duration.ofSeconds(105));

            // assertions
            List<FraudCase> fraudCases = outputTopic.readValuesToList();
            Assertions.assertEquals(1, fraudCases.size());
            Assertions.assertEquals(5, fraudCases.get(0).getMovements().size());
            Assertions.assertEquals(380, fraudCases.get(0).getTotalAmount());
            Assertions.assertEquals(PHYSICAL_FRAUD_DESCRIPTION, fraudCases.get(0).getDescription());
            Assertions.assertEquals("c1", fraudCases.get(0).getCard());

        }

    }

}
