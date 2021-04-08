/**
 * 
 */
package pucrs.antunes.causalLog.recovery.model;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
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
	protected ConcurrentHashMap<Integer, Integer> replicaMap = new ConcurrentHashMap<Integer, Integer>();
	protected AtomicInteger iterations = new AtomicInteger(0);
	protected byte[][] recoveryLog;
	protected int nThreads;
	protected ExecutorService pool;

	public RecoveryModel(byte[][] recoveryLog, int threads) {
		this.recoveryLog = recoveryLog;
		this.nThreads = threads;
	}

	protected Integer execute(KvsCmd cmd, Map<Integer, Integer> state) {
		return cmd.getType().execute(state, cmd);
	}

	public abstract void executeWorkflow();
}
