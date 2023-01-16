package net.wrmay.jetdemo;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.Random;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Expects the following environment variables
 *
 * PULSAR_URL    The URL of a Pulsar broker
 *
 * PULSAR_TOPIC  The topic to which events will be published.
 *
 * MACHINE_COUNT The number of machines to emulate.
 */
public class EventGenerator {

    public static final String PULSAR_URL_PROP = "PULSAR_URL";

    public static final String PULSAR_TOPIC_PROP = "PULSAR_TOPIC";
    public static final String MACHINE_COUNT_PROP = "MACHINE_COUNT";

    public static final String RUNHOT_PROP = "RUNHOT";


    private static String pulsarURL;

    private static String pulsarTopic;
    private static int machineCount;

    private static boolean runHot;

    private static String getRequiredProp(String propName){
        String prop = System.getenv(propName);
        if (prop == null){
            System.err.println("The " + propName + " property must be set");
            System.exit(1);
        }
        return prop;
    }

    // guarantees to return a result or call System.exit
    private static int getRequiredIntegerProp(String propName){
        String temp = getRequiredProp(propName);
        int result = 0;
        try {
            result = Integer.parseInt(temp);
        } catch(NumberFormatException nfx){
            System.err.println("Could not parse " + temp + " as an integer");
            System.exit(1);
        }

        return  result;
    }

    private static void configure(){
        pulsarURL = getRequiredProp(PULSAR_URL_PROP);
        pulsarTopic = getRequiredProp(PULSAR_TOPIC_PROP);

        machineCount = getRequiredIntegerProp(MACHINE_COUNT_PROP);

        if (machineCount < 1 || machineCount > 1000000){
            System.err.println("Machine count must be between 1 and 1,000,000 inclusive");
            System.exit(1);
        }

        String str = System.getenv(RUNHOT_PROP);
        if (str == null){
            runHot = false;
        } else {
            str = str.toLowerCase();
            runHot = str.equals("yes") || str.equals("true");
        }
    }

    public static void main(String []args){
        configure();

        try(PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(pulsarURL).build()){
            try(Producer<byte[]> producer = pulsarClient.newProducer().blockIfQueueFull(true).topic(pulsarTopic).create()){

            }

            IMap<String, MachineStatus> machineEventMap = hzClient.getMap(Names.EVENT_MAP_NAME);

            int existingEntries = machineProfileMap.size();

            while (existingEntries < machineCount){
                System.out.println("waiting for at least " + machineCount + " machine profiles to be loaded");
                try {
                    Thread.sleep(4000);
                } catch(InterruptedException x){
                    // ?
                }
                existingEntries = machineProfileMap.size();
            }

            // add some sleep to prevent the condition where the loader has not finished initializing the sql mapping
            try {
                Thread.sleep(5000);
            } catch(InterruptedException x){
                //
            }

            // now we have sufficient profiles to start generating data
            String[] serialNums = new String[machineCount];
            try(SqlResult result = hzClient.getSql().execute("SELECT serialNum FROM " + Names.PROFILE_MAP_NAME +  " WHERE serialNum != ? LIMIT ?", Names.SPECIAL_SN, machineCount)) {
                int i = 0;
                for (SqlRow row : result) {
                    serialNums[i++] = row.getObject(0);
                }
                if (i < machineCount) {
                    System.err.println("Could not retrieve sufficient profiles from the " + Names.PROFILE_MAP_NAME + " map.");
                    System.exit(1);
                }
            }

            Random rand = new Random();
            ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(machineCount / 100, new DaemonThreadFactory());
            try(Closer<ScheduledThreadPoolExecutor> threadPoolExecutorCloser = new Closer<>(executor, ScheduledThreadPoolExecutor::shutdown)){

                if (runHot){
                    executor.scheduleAtFixedRate(new MachineEmulator(machineEventMap,Names.SPECIAL_SN, true),0, 1000, TimeUnit.MILLISECONDS);
                } else {
                    MachineEmulator[] machineEmulators = new MachineEmulator[machineCount];
                    for(int j=0;j< machineCount; ++j){
                        machineEmulators[j] = new MachineEmulator(machineEventMap, serialNums[j], false);
                        executor.scheduleAtFixedRate(machineEmulators[j], rand.nextInt(1000), 1000, TimeUnit.MILLISECONDS);
                    }
                }

                AtomicBoolean running = new AtomicBoolean(true);

                Runtime.getRuntime().addShutdownHook(new Thread(() -> running.set(false)));

                while(running.get()){
                    try {
                        Thread.sleep(2000);
                    } catch(InterruptedException ix){
                        break;
                    }
                }
                System.out.println("Shutting down");
            } // close thread pool
        }  // close Hazelcast instance
        catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }

    }

    private static class Closer<T> implements AutoCloseable {

        private final T client;
        private final Consumer<T> closeFn;

        public Closer(T hc, Consumer<T> closeFn){
            this.client = hc;
            this.closeFn = closeFn;
        }

        @Override
        public void close()  {
            closeFn.accept(client);
        }
    }

    private static class DaemonThreadFactory implements ThreadFactory {

        @Override
        public Thread newThread(Runnable r) {
            Thread result = new Thread(r);
            result.setDaemon(true);
            return result;
        }
    }
}
