package pucrs.antunes.causalLog.utils;

import java.util.function.Supplier;

public class SyntacticDelay {

    // NB. Volatile updates to a public var prevents the JVM
    // from eliding the code used to produce busy wait.
    public volatile int dummyVar;
    private final int minCostNS;

    public SyntacticDelay(int minCostNS) {
        this.minCostNS = minCostNS;
    }

    public <T> T ensureMinCost(Supplier<T> fn) {
        long start = System.nanoTime();
        T result = fn.get();
        while (System.nanoTime() < start + minCostNS)
            dummyVar++;
        return result;
    }
}