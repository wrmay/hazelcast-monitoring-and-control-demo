package net.wrmay.jetdemo;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class LoggingSinkBuilder {

    private static final String PRINTWRITER_ARG = "pw";
    public static <T> Sink<T> buildSink(String name, String logDir, FunctionEx<T, String> toStringFn){
        // the create function
        SinkBuilder<Printer<T>,Void> sb = SinkBuilder.sinkBuilder(name, ctx -> {
            JobConfig jc = ctx.jobConfig();
            CloseableRef<PrintWriter> pwref;
            synchronized (jc){
                pwref = jc.getArgument(PRINTWRITER_ARG);
                if (pwref == null){
                    pwref = new CloseableRef<>(initPrintWriter(logDir));
                    jc.setArgument(PRINTWRITER_ARG, pwref);
                }
            }
            return new Printer<>(toStringFn, pwref);
        });

        SinkBuilder<Printer<T>, T> sb2 = sb.<T>receiveFn(Printer::print).destroyFn(Printer::close);
        sb2.preferredLocalParallelism(1);

        return sb2.build();
    }

    private static PrintWriter initPrintWriter(String logDir){
        PrintWriter result;
        Iterator< HazelcastInstance> iter = Hazelcast.getAllHazelcastInstances().iterator();
        if (!iter.hasNext()){
            throw new RuntimeException("Could not obtain a Hazelcast instance");
        }
        String name = iter.next().getName() + ".log";

        File outputDir = new File(logDir);
        if (!outputDir.isDirectory()){
            throw new RuntimeException("Could not initialize LoggingSink because \"" +
                    logDir + "\" is not a directory");
        }
        if (!outputDir.canWrite()){
            throw new RuntimeException("Could not initialize LoggingSink because \"" +
                    logDir + "\" is not writeable");

        }

        File outFile = new File(outputDir, name);

        try {
            result = new PrintWriter(
                    new OutputStreamWriter(
                            new FileOutputStream(outFile, false), StandardCharsets.UTF_8), true);
        } catch(IOException iox){
            throw new RuntimeException("Could not initialize Logging Sink", iox);
        }
        return result;
    }

    /**
     * Printer is the Context object for this Sink.  Its state is a PrintWriter and a
     * toString function.  All Sinks created by this class use the same PrintWriter
     * and log to the same file.  A reference counting scheme is used to make sure that
     * the PrintWriter is closed when the last Sink is destroyed.
     * @param <T>
     */
    public static class Printer<T> implements Serializable {
        private final FunctionEx<T, String> toStringFn;
        private final CloseableRef<PrintWriter> printWriter;

        public Printer(FunctionEx<T, String> toStringFn, CloseableRef<PrintWriter> pw){
            this.toStringFn = toStringFn;
            this.printWriter = pw;
            this.printWriter.acquire();
        }

        public   void print(T thing){
            printWriter.get().println(toStringFn.apply(thing));
        }

        public void close(){
            this.printWriter.release();
        }
    }
}
