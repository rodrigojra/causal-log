/**
 * 
 */
package pucrs.antunes.causalLog.recovery.model;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import pucrs.antunes.causalLog.recovery.map.KvsCmd;
import pucrs.antunes.causalLog.utils.SyntacticDelay;

/**
 * @author rodrigo
 *
 */
public abstract class RecoveryModel {

	static final class Task {
		final KvsCmd request;
		final CompletableFuture<Void> future;

		Task(KvsCmd request) {
			this.request = request;
			this.future = new CompletableFuture<>();
		}
	}

	SyntacticDelay delay = new SyntacticDelay(100000000);
	protected ConcurrentHashMap<Integer, Integer> replicaMap;
	protected AtomicInteger iterations = new AtomicInteger(0);
	protected byte[][] recoveryLog;
	//TODO tem que virar parametro no construtor
	private static final int nThreads = 2;
	protected ExecutorService pool = new ForkJoinPool(nThreads, ForkJoinPool.defaultForkJoinWorkerThreadFactory, null,
			true, nThreads, nThreads, 0, null, 60, TimeUnit.SECONDS);

	public RecoveryModel(byte[][] recoveryLog) {
		this.recoveryLog = recoveryLog;
	}

	protected Integer execute(KvsCmd cmd, Map<Integer, Integer> state) {
		return cmd.getType().execute(state, cmd);
	}

	public abstract void executeWorkflow();
}
