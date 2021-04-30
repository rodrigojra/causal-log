/**
 * 
 */
package pucrs.antunes.causalLog.recovery.model;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;

import pucrs.antunes.causalLog.recovery.map.KvsCmd;

/**
 * @author Rodrigo Antunes
 *
 */
public class Sequential extends RecoveryModel {

	public Sequential(ArrayList<KvsCmd> recoveryLog, int threads, int delayTime) {
		super(recoveryLog, threads, delayTime);
		System.out.println("Executing sequential model workload size: " + recoveryLog.size() +" number of threads: "+threads);
	}

	@Override
	public void executeWorkflow() {
		Stopwatch stopwatch = Stopwatch.createStarted();

		for (KvsCmd cmdFromLog : recoveryLog) {
			execute(cmdFromLog);
		}

		stopwatch.stop();
		System.out.println("Recovery time elapsed MILLISECONDS: " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
		System.out.println("Recovery time elapsed SECONDS: " + stopwatch.elapsed(TimeUnit.SECONDS));
	}

	private byte[] execute(KvsCmd cmd) {
		return delay.ensureMinCost(() -> {
			ByteBuffer resp = ByteBuffer.allocate(4);
			Integer cmdResult = execute(cmd, replicaMap);
			if (cmdResult == null) {
				cmdResult = Integer.MIN_VALUE;
			}
			resp.putInt(cmdResult);
			iterations.incrementAndGet();
			// flagLastExecuted.set(task.request.getId());
			return resp.array();
		});
	}
}
