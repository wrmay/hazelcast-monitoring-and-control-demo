package net.wrmay.jetdemo;

import com.hazelcast.map.IMap;

public class MachineEmulator implements  Runnable{
    private final String serialNum;

    private final SignalGenerator tempSignalGenerator;
    private final IMap<String, MachineStatus> machineStatusIMap;

    private int t;
    public MachineEmulator(IMap<String, MachineStatus> machineStatusIMap, String sn, boolean isHot){
        System.out.println("Initializing machine emulator S/N: " + sn + " HOT: " + isHot);
        this.serialNum = sn;
        this.machineStatusIMap = machineStatusIMap;
        this.t = 0;

        if (isHot)
            tempSignalGenerator = new SignalGenerator(95f,.5f,2f);
        else
            tempSignalGenerator = new SignalGenerator(95f, 0f, 1f);
    }

    @Override
    public void run() {
        MachineStatus status = new MachineStatus();
        status.setSerialNum(serialNum);
        status.setTimestamp(System.currentTimeMillis());
        status.setBitTemp(tempSignalGenerator.compute(t++));
        status.setBitRPM(10000);
        status.setBitPositionX(0);
        status.setBitBitPositionY(0);
        status.setBitPositionZ(0);

        machineStatusIMap.put(serialNum, status);
    }
}
