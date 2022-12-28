package net.wrmay.jetdemo;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.*;

import java.util.Map;

public class TemperatureMonitor1 {
    public static void main(String []args){
        if (args.length != 1){
            System.err.println("Please provide the log output directory as the first argument");
            System.exit(1);
        }

        String logDir = args[0];

        Pipeline pipeline = Pipeline.create();

        Sink<Map.Entry<String,MachineStatus>> sink = LoggingSinkBuilder.buildSink("log events",
                logDir,
                entry ->  "EVENT (" + entry.getKey() + ")");

        // create the map journal source
        StreamSource<Map.Entry<String, MachineStatus>> machineStatusEventSource
                = Sources.mapJournal(Names.EVENT_MAP_NAME, JournalInitialPosition.START_FROM_CURRENT);

        // create a stream of MachineStatus events from the map journal, use the timestamps embedded in the events
        StreamStage<Map.Entry<String, MachineStatus>> statusEvents = pipeline.readFrom(machineStatusEventSource)
                        .withTimestamps(event -> event.getValue().getTimestamp(), 2000)
                        .setName("machine status events");

        // log the events
        statusEvents.writeTo(sink);

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("Temperature Monitor");
        HazelcastInstance hz = Hazelcast.bootstrappedInstance();
        hz.getJet().newJob(pipeline, jobConfig);
    }
}
