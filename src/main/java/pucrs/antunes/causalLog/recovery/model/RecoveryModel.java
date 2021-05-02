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
		final KvsCmd cmd;
		final CompletableFuture<Void> future;

		Task(KvsCmd cmd) {
			this.cmd = cmd;
			this.future = new CompletableFuture<>();
		}
	}

	SyntacticDelay delay;
	protected ConcurrentHashMap<Integer, Integer> replicaMap = new ConcurrentHashMap<Integer, Integer>();
	protected AtomicInteger iterations = new AtomicInteger(0);
	protected ArrayList<KvsCmd> recoveryLog;
	protected int nThreads;
	protected ExecutorService pool;
	protected int delayTime;

	public RecoveryModel(ArrayList<KvsCmd> recoveryLog, int threads, int delayTime) {
		this.recoveryLog = recoveryLog;
		this.nThreads = threads;
		this.delayTime = delayTime;
		this.delay = new SyntacticDelay(delayTime);
	}

	protected Integer execute(KvsCmd cmd, Map<Integer, Integer> state) {
		return cmd.getType().execute(state, cmd);
	}

	public abstract void executeWorkflow();
}
