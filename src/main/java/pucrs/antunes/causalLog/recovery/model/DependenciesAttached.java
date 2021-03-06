/**
 * 
 */
package pucrs.antunes.causalLog.recovery.model;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;

import pucrs.antunes.causalLog.recovery.map.KvsCmd;

/**
 * @author Rodrigo Antunes
 *
 */
public class DependenciesAttached extends RecoveryModel {

	public DependenciesAttached(ArrayList<KvsCmd> recoveryLog, int threads, int delayTime) {
		super(recoveryLog, threads, delayTime);
		pool = new ForkJoinPool(nThreads, ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true, nThreads,
				nThreads, 0, null, 60, TimeUnit.SECONDS);
		System.out.println("Executing attached model workload size: " + recoveryLog.size() +" number of threads: "+threads);
	}

	private HashMap<Long, Task> scheduledMap = new HashMap<Long, Task>();

	@Override
	public void executeWorkflow() {
		// Recovery from log
		Stopwatch stopwatch = Stopwatch.createStarted();
		ListIterator<KvsCmd> iterator = recoveryLog.listIterator();

		while (iterator.hasNext()) {
			KvsCmd cmdFromLog = iterator.next();
			Task newTask = new Task(cmdFromLog);
			addTaskMap(newTask);
			List<CompletableFuture<Void>> dependencies = new LinkedList<>();

			if (newTask.cmd.getDependencies() != null && !newTask.cmd.getDependencies().isEmpty()) {
				Task dependentTask = null;
				ListIterator<KvsCmd> iterDependencies = newTask.cmd.getDependencies().listIterator();
				while (iterDependencies.hasNext()) {
					KvsCmd cmdDependency = iterDependencies.next();

					if (scheduledMap.containsKey(cmdDependency.getId())) {
						dependentTask = scheduledMap.get(cmdDependency.getId());
					}

					if (!dependentTask.future.isDone()) {
						dependencies.add(dependentTask.future);
					}
				}
				submit(newTask, dependencies);
			} else {
				pool.execute(() -> execute(newTask));
			}
		}

		pool.shutdown();
		// next line will block till all tasks finishes
		try {
			pool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		stopwatch.stop();
		System.out.println("Recovery time elapsed MILLISECONDS: " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
		System.out.println("Recovery time elapsed SECONDS: " + stopwatch.elapsed(TimeUnit.SECONDS));
	}

	private void addTaskMap(Task newTask) {
		scheduledMap.put(newTask.cmd.getId(), newTask);
	}

	private void submit(Task newTask, List<CompletableFuture<Void>> dependencies) {
		if (dependencies.isEmpty()) {
			// System.out.println("submit pool =>" + newTask.cmd.getId());
			pool.execute(() -> execute(newTask));
			// execute(newTask);
		} else {
			// System.out.println("submit cf => " + newTask.cmd.getId());
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
			Integer cmdResult = execute(task.cmd, replicaMap);
			if (cmdResult == null) {
				cmdResult = Integer.MIN_VALUE;
			}
			resp.putInt(cmdResult);
			iterations.incrementAndGet();
			// flagLastExecuted.set(task.cmd.getId());
			task.future.complete(null);
			return resp.array();
		});
	}
}
