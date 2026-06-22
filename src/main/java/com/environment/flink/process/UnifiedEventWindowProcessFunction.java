package com.environment.flink.process;

import com.environment.flink.model.AggregateAccumulator;
import com.environment.flink.model.FeatureSnapshot;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class UnifiedEventWindowProcessFunction
        extends ProcessWindowFunction<AggregateAccumulator, FeatureSnapshot, String, TimeWindow> {

    @Override
    public void process(
            String h3Cell,
            Context context,
            Iterable<AggregateAccumulator> elements,
            Collector<FeatureSnapshot> out
    ) {
        AggregateAccumulator acc = elements.iterator().next();

        acc.setWindowStart(context.window().getStart());
        acc.setWindowEnd(context.window().getEnd());

        FeatureSnapshot snapshot = FeatureSnapshotBuilder.build(
                h3Cell,
                acc,
                context.window().getStart(),
                context.window().getEnd()
        );

        out.collect(snapshot);
    }
}