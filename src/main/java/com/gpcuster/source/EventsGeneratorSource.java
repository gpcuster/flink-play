package com.gpcuster.source;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A event stream source that generates the events on the fly. Useful for
 * self-contained demos.
 */
@SuppressWarnings("serial")
public class EventsGeneratorSource extends RichParallelSourceFunction<String> {
    private final int delayPerRecordMillis;

    private volatile boolean running = true;

    public EventsGeneratorSource(int delayPerRecordMillis) {
        checkArgument(delayPerRecordMillis >= 0, "delay must be >= 0");

        this.delayPerRecordMillis = delayPerRecordMillis;
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        final int range = Integer.MAX_VALUE / getRuntimeContext().getNumberOfParallelSubtasks();
        final int min = range * getRuntimeContext().getIndexOfThisSubtask();
        final int max = min + range;

        while (running) {
            String event = "hello from" + getRuntimeContext().getIndexOfThisSubtask();
            sourceContext.collect(event);

            if (delayPerRecordMillis > 0) {
                Thread.sleep(delayPerRecordMillis);
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}

