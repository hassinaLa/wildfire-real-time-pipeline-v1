package com.environment.flink.process;

import com.environment.flink.model.AggregateAccumulator;
import com.environment.flink.model.UnifiedEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

public class UnifiedEventWindowAggregateFunction
        implements AggregateFunction<UnifiedEvent, AggregateAccumulator, AggregateAccumulator> {

    @Override
    public AggregateAccumulator createAccumulator() {
        return new AggregateAccumulator();
    }

    @Override
    public AggregateAccumulator add(UnifiedEvent value, AggregateAccumulator acc) {
        acc.add(value);
        return acc;
    }

    @Override
    public AggregateAccumulator getResult(AggregateAccumulator acc) {
        return acc;
    }

    @Override
    public AggregateAccumulator merge(AggregateAccumulator a, AggregateAccumulator b) {
        a.merge(b);
        return a;
    }
}