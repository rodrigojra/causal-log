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

	public Sequential(byte[][] recoveryLog) {
		super(recoveryLog);
	}

	@Override
	public void executeWorkflow() {
		// Recovery from log
		Stopwatch stopwatch = Stopwatch.createStarted();
		for (int i = 0; i < recoveryLog.length; i++) {
			byte[] bs = recoveryLog[i];
			KvsCmd cmdFromLog = Utils.byteArrayToCmd(bs);
			Task newTask = new Task(cmdFromLog);
			execute(newTask);
		}
		stopwatch.stop();
		System.out.println("Recovery time elapsed: " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
		System.out.println("Recovery time elapsed: " + stopwatch.elapsed(TimeUnit.SECONDS));
	}

	private byte[] execute(Task task) {
		return delay.ensureMinCost(() -> {
			System.out.println(task.request.getId());
			ByteBuffer resp = ByteBuffer.allocate(4);
			resp.putInt(execute(task.request, replicaMap));
			iterations.incrementAndGet();
			// flagLastExecuted.set(task.request.getId());
			return resp.array();
		});
	}
}
