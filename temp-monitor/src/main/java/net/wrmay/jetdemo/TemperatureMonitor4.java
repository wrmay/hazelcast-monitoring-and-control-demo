package net.wrmay.jetdemo;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple4;
import com.hazelcast.jet.datamodel.Tuple5;
import com.hazelcast.jet.pipeline.*;

import java.util.Map;

public class TemperatureMonitor4 {

    private static String categorizeTemp(double temp, int warningLimit, int criticalLimit){
        String result;
        if (temp > (double) criticalLimit)
            result = "RED";
        else if (temp > (double) warningLimit)
            result = "ORANGE";
        else
            result = "GREEN";

        return result;
    }

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

        // look up the machine profile for this machine, copy the warning temp onto the event
        // the output is serial number, avg temp, warning temp, critical temp
        StreamStage<Tuple4<String, Double, Integer, Integer>> temperaturesAndLimits = averageTemps.groupingKey(KeyedWindowResult::getKey).<MachineProfile, Tuple4<String, Double, Integer, Integer>>mapUsingIMap(
                        Names.PROFILE_MAP_NAME,
                        (window, machineProfile) -> Tuple4.tuple4(window.getKey(), window.getValue(), machineProfile.getWarningTemp(), machineProfile.getCriticalTemp()))
                .setName("Lookup Temp Limits");

        // categorize as GREEN / ORANGE / RED, add category to the end of the existing tuple
        StreamStage<Tuple5<String, Double, Integer, Integer, String>> labeledTemperatures = temperaturesAndLimits.map(tuple -> Tuple5.tuple5(tuple.f0(), tuple.f1(), tuple.f2(), tuple.f3(), categorizeTemp(tuple.f1(), tuple.f2(), tuple.f3())))
                .setName("Apply Label");

        labeledTemperatures.writeTo(Sinks.logger());

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("Temperature Monitor");
        HazelcastInstance hz = Hazelcast.bootstrappedInstance();
        hz.getJet().newJob(pipeline, jobConfig);
    }
}
