package net.wrmay.jetdemo;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.pipeline.*;

import java.util.Map;

public class TemperatureMonitor2 {
    public static void main(String []args){
        Pipeline pipeline = Pipeline.create();

        // create the map journal source
        StreamSource<Map.Entry<String, MachineStatus>> machineStatusEventSource
                = Sources.mapJournal(Names.EVENT_MAP_NAME, JournalInitialPosition.START_FROM_CURRENT);

        // create a stream of MachineStatus events from the map journal, use the timestamps embedded in the events
        StreamStage<Map.Entry<String, MachineStatus>> statusEvents = pipeline.readFrom(machineStatusEventSource)
                        .withTimestamps(event -> event.getValue().getTimestamp(), 2000)
                        .setName("machine status events");

        // split the events by serial number, create a tumbling window to calculate avg. temp over 10s
        // output is a tuple: serial number, avg temp

        StreamStage<KeyedWindowResult<String, Double>> averageTemps = statusEvents.groupingKey(entry -> entry.getValue().getSerialNum())
                .window(WindowDefinition.tumbling(10000))
                .aggregate(AggregateOperations.averagingLong(event -> event.getValue().getBitTemp())).setName("Average Temp");

        // log them
        averageTemps.writeTo(Sinks.logger());

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("Temperature Monitor");
        HazelcastInstance hz = Hazelcast.bootstrappedInstance();
        hz.getJet().newJob(pipeline, jobConfig);
    }
}
