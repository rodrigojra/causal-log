/**
 * 
 */
package pucrs.antunes.causalLog.recovery.model;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;

import pucrs.antunes.causalLog.recovery.map.KvsCmd;
import pucrs.antunes.causalLog.utils.Utils;

/**
 * @author Rodrigo Antunes
 *
 */
public class CreateDependencyTree extends RecoveryModel {

	private final List<Task> scheduled = new LinkedList<>();

	public CreateDependencyTree(ArrayList<KvsCmd> recoveryLog, int threads) {
		super(recoveryLog, threads);
		pool = new ForkJoinPool(nThreads, ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true, nThreads,
				nThreads, 0, null, 60, TimeUnit.SECONDS);
		System.out.println("Executing graph model workload size: " + recoveryLog.size() +" number of threads: "+threads);
		
	}

	@Override
	public void executeWorkflow() {
		// Recovery from log
		Stopwatch stopwatch = Stopwatch.createStarted();
		for (KvsCmd cmdFromLog : recoveryLog) {
			doSchedule(cmdFromLog);
		}
//		for (int i = 0; i < recoveryLog.length; i++) {
//			byte[] bs = recoveryLog[i];
//			KvsCmd cmdFromLog = Utils.byteArrayToCmd(bs);
//			doSchedule(cmdFromLog);
//		}
		pool.shutdown();
		// next line will block till all tasks finishes
		try {
			pool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		stopwatch.stop();
		System.out.println("Recovery time elapsed MILLISECONDS: " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
		System.out.println("Recovery time elapsed SECONDS: " + stopwatch.elapsed(TimeUnit.SECONDS));
	}

	private void doSchedule(KvsCmd cmd) {
		Task newTask = new Task(cmd);
		submit(newTask, addTask(newTask));
	}

	private List<CompletableFuture<Void>> addTask(Task newTask) {
		List<CompletableFuture<Void>> dependencies = new LinkedList<>();
		ListIterator<Task> iterator = scheduled.listIterator();

		while (iterator.hasNext()) {
			Task task = iterator.next();

			if (task.future.isDone()) {
				iterator.remove();
				continue;
			}

			if (Utils.conflictWith(newTask.request, task.request)) {
				dependencies.add(task.future);
			}
		}

		scheduled.add(newTask);
		return dependencies;
	}

	private void submit(Task newTask, List<CompletableFuture<Void>> dependencies) {
		if (dependencies.isEmpty()) {

			pool.execute(() -> execute(newTask));
		} else {

			after(dependencies).thenRun(() -> {

				execute(newTask);
			});
		}
	}

	private static CompletableFuture<Void> after(List<CompletableFuture<Void>> fs) {
		if (fs.size() == 1)
			return fs.get(0); // fast path
		return CompletableFuture.allOf(fs.toArray(new CompletableFuture[0]));
	}

	private byte[] execute(Task task) {
		return delay.ensureMinCost(() -> {
			ByteBuffer resp = ByteBuffer.allocate(4);
			Integer cmdResult = execute(task.request, replicaMap);
			if (cmdResult == null) {
				cmdResult = Integer.MIN_VALUE;
			}
			resp.putInt(cmdResult);
			iterations.incrementAndGet();
			// flagLastExecuted.set(task.request.getId());
			task.future.complete(null);
			return resp.array();
		});
	}
}
