package net.wrmay.jetdemo;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.*;

import java.util.Map;

public class TemperatureMonitor1 {
    public static void main(String []args){
        Pipeline pipeline = Pipeline.create();

        // create the map journal source
        StreamSource<Map.Entry<String, MachineStatus>> machineStatusEventSource
                = Sources.mapJournal(Names.EVENT_MAP_NAME, JournalInitialPosition.START_FROM_CURRENT);

        // create a stream of MachineStatus events from the map journal, use the timestamps embedded in the events
        StreamStage<Map.Entry<String, MachineStatus>> statusEvents = pipeline.readFrom(machineStatusEventSource)
                        .withTimestamps(event -> event.getValue().getTimestamp(), 2000)
                        .setName("machine status events");

        // log the events
        statusEvents.writeTo(Sinks.logger());

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("Temperature Monitor");
        HazelcastInstance hz = Hazelcast.bootstrappedInstance();
        hz.getJet().newJob(pipeline, jobConfig);
    }
}
