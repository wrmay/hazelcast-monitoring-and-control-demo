package net.wrmay.jetdemo;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import java.util.HashMap;
import java.util.Map;

/**
 * Expects the following environment variables
 *
 * HZ_SERVERS  A comma-separated list of Hazelcast servers in host:port format.  Port may be omitted.
 *             Any whitespace around the commas will be removed.  Required.
 *
 * HZ_CLUSTER_NAME  The name of the Hazelcast cluster to connect.  Required.
 */
public class RefdataLoader {
    public static final String HZ_SERVERS_PROP = "HZ_SERVERS";
    public static final String HZ_CLUSTER_NAME_PROP = "HZ_CLUSTER_NAME";

    private static String []hzServers;
    private static String hzClusterName;

    private static String getRequiredProp(String propName){
        String prop = System.getenv(propName);
        if (prop == null){
            System.err.println("The " + propName + " property must be set");
            System.exit(1);
        }
        return prop;
    }

    private static void configure(){
        String hzServersProp = getRequiredProp(HZ_SERVERS_PROP);
        hzServers = hzServersProp.split(",");
        for(int i=0; i < hzServers.length; ++i) hzServers[i] = hzServers[i].trim();

        hzClusterName = getRequiredProp(HZ_CLUSTER_NAME_PROP);
    }

    public static void main(String []args){
        configure();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName(hzClusterName);
        clientConfig.getNetworkConfig().addAddress(hzServers);

        // enable compact serialization
        clientConfig.getSerializationConfig().getCompactSerializationConfig().setEnabled(true);

        HazelcastInstance hzClient = HazelcastClient.newHazelcastClient(clientConfig);

        Map<String, MachineProfile> batch = new HashMap<>();
        IMap<String, MachineProfile> machineProfileMap = hzClient.getMap("machine_profiles");

        for(int i=0;i< 1000; ++i){
            MachineProfile mp = MachineProfile.fake();
            batch.put(mp.getSerialNum(), mp);
        }
        machineProfileMap.putAll(batch);

        System.out.println("Loaded machine profiles");
        hzClient.shutdown();
    }
}
