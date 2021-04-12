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

	public DependenciesAttached(ArrayList<KvsCmd> recoveryLog, int threads) {
		super(recoveryLog, threads);
		pool = new ForkJoinPool(nThreads, ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true, nThreads,
				nThreads, 0, null, 60, TimeUnit.SECONDS);
		System.out.println("Executing dependencies attached model...");
	}

	private HashMap<Long, Task> scheduledMap = new HashMap<Long, Task>();

	@Override
	public void executeWorkflow() {
		// Recovery from log
		Stopwatch stopwatch = Stopwatch.createStarted();
		ListIterator<KvsCmd> iterator = recoveryLog.listIterator();

		while (iterator.hasNext()) {
			KvsCmd cmdFromLog = iterator.next();
//		for (int i = 0; i < recoveryLog.length; i++) {
//			byte[] bs = recoveryLog[i];
//			KvsCmd cmdFromLog = Utils.byteArrayToCmd(bs);
			Task newTask = new Task(cmdFromLog);
			addTaskMap(newTask);
			List<CompletableFuture<Void>> dependencies = new LinkedList<>();

			if (newTask.request.getDependencies() != null && !newTask.request.getDependencies().isEmpty()) {
				Task dependentTask = null;
				ListIterator<KvsCmd> iterDependencies = newTask.request.getDependencies().listIterator();
				while (iterDependencies.hasNext()) {
					KvsCmd cmdDependency = iterDependencies.next();
//				}
//				for (KvsCmd cmdDependency : newTask.request.getDependencies()) {
					if (scheduledMap.containsKey(cmdDependency.getId())) {
						dependentTask = scheduledMap.get(cmdDependency.getId());
					} // else {
						// Task depTask = new Task(cmdDependency);
						// scheduledMap.put(cmdDependency.getId(), depTask);
						// t = depTask;
						// }
					if (!dependentTask.future.isDone()) {
						dependencies.add(dependentTask.future);
					}

					// se ele nao estiver no map pode ser um erro, pode ser um comando de outro
					// batch/checkpoint
					// isso deixaria o comando orfao, como temos o comando, podemos forçar a
					// execuçao dele
					// O problema 'e que se o comando foi executado, ele sera executado de novo.
				}
				submit(newTask, dependencies);
			} else {
				pool.execute(() -> execute(newTask));
				// execute(newTask);
			}

			// submit(newTask, dependencies);
		}

		pool.shutdown();
		// next line will block till all tasks finishes
		try {
			pool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		stopwatch.stop();
		System.out.println("Recovery time elapsed: " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
		System.out.println("Recovery time elapsed: " + stopwatch.elapsed(TimeUnit.SECONDS));
	}

	private void addTaskMap(Task newTask) {
		scheduledMap.put(newTask.request.getId(), newTask);
	}

	private void submit(Task newTask, List<CompletableFuture<Void>> dependencies) {
		if (dependencies.isEmpty()) {
			// System.out.println("submit pool =>" + newTask.request.getId());
			pool.execute(() -> execute(newTask));
			// execute(newTask);
		} else {
			// System.out.println("submit cf => " + newTask.request.getId());
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
