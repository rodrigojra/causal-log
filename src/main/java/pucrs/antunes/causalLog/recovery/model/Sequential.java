/**
 * 
 */
package pucrs.antunes.causalLog.recovery.model;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;

import pucrs.antunes.causalLog.recovery.map.KvsCmd;
import pucrs.antunes.causalLog.utils.Utils;

/**
 * @author Rodrigo Antunes
 *
 */
public class Sequential extends RecoveryModel {

	public Sequential(byte[][] recoveryLog, int threads) {
		super(recoveryLog, threads);
		System.out.println("Executing sequential model...");
	}

	@Override
	public void executeWorkflow() {
		// Recovery from log
		Stopwatch stopwatch = Stopwatch.createStarted();
		for (int i = 0; i < recoveryLog.length; i++) {
			byte[] bs = recoveryLog[i];
			KvsCmd cmdFromLog = Utils.byteArrayToCmd(bs);
			execute(cmdFromLog);
		}
		stopwatch.stop();
		System.out.println("Recovery time elapsed: " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
		System.out.println("Recovery time elapsed: " + stopwatch.elapsed(TimeUnit.SECONDS));
	}

	private byte[] execute(KvsCmd cmd) {
		return delay.ensureMinCost(() -> {
			ByteBuffer resp = ByteBuffer.allocate(4);
			Integer cmdResult =  execute(cmd, replicaMap);
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
