/**
 * 
 */
package pucrs.antunes.causalLog.recovery.model;

import java.util.ArrayList;
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
	
	public enum Models {
		SEQUENTIAL("SEQUENTIAL"),
		GRAPH("GRAPH"),
		ATTACHED("ATTACHED");

		private final String model;
		Models(String string) {
			this.model = string;
		}
		
	    @Override
	    public String toString() {
	        return this.model;
	    }		
	}

	static final class Task {
		final KvsCmd request;
		final CompletableFuture<Void> future;

		Task(KvsCmd request) {
			this.request = request;
			this.future = new CompletableFuture<>();
		}
	}

	SyntacticDelay delay = new SyntacticDelay(1000000);
	protected ConcurrentHashMap<Integer, Integer> replicaMap = new ConcurrentHashMap<Integer, Integer>();
	protected AtomicInteger iterations = new AtomicInteger(0);
	protected ArrayList<KvsCmd> recoveryLog;
	protected int nThreads;
	protected ExecutorService pool;

	public RecoveryModel(ArrayList<KvsCmd> recoveryLog, int threads) {
		this.recoveryLog = recoveryLog;
		this.nThreads = threads;
	}

	protected Integer execute(KvsCmd cmd, Map<Integer, Integer> state) {
		return cmd.getType().execute(state, cmd);
	}

	public abstract void executeWorkflow();
}
