package pucrs.antunes.causalLog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import com.google.common.base.Stopwatch;

import pucrs.antunes.causalLog.recovery.map.KvsCmd;
import pucrs.antunes.causalLog.utils.SyntacticDelay;
import pucrs.antunes.causalLog.utils.Utils;

public class UtilsTest {
	SyntacticDelay delay;
	private static final int nThreads = 2;
	private final List<Task> scheduled = new LinkedList<>();
	private HashMap<Long, Task>  scheduledMap = new HashMap<Long, UtilsTest.Task>();
	
	ExecutorService pool = new ForkJoinPool(nThreads, ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true,
			nThreads, nThreads, 0, null, 60, TimeUnit.SECONDS);

	AtomicLong flagLastExecuted = new AtomicLong();
	int n = 10;
	
	byte[][] logRequests = generateCommands(n);
	
	
	private static final class Task {
		private final KvsCmd request;
		private final CompletableFuture<Void> future;

		Task(KvsCmd request) {
			this.request = request;
			this.future = new CompletableFuture<>();
		}
	}

	private List<CompletableFuture<Void>> addTask(Task newTask) {
		List<CompletableFuture<Void>> dependencies = new LinkedList<>();
		ListIterator<Task> iterator = scheduled.listIterator();
		ListIterator<KvsCmd> itCmd = null;

		while (iterator.hasNext()) {
			Task task = iterator.next();
			if (task.future.isDone()) {
				iterator.remove();
				continue;
			}
			
//			if (conflict.isDependent(task.request, newTask.request)) {
//				dependencies.add(task.future);
//			}

			if (!newTask.request.getDependencies().isEmpty()) {
				itCmd = newTask.request.getDependencies().listIterator();
				while (itCmd.hasNext()) {
					KvsCmd appCmd =  itCmd.next();
					if (appCmd.getId() == task.request.getId()) {
						itCmd.remove();
					}
				}
			}
		}
		
		scheduled.add(newTask);
		return dependencies;
	}

	private void submit(Task newTask, List<CompletableFuture<Void>> dependencies) {
		if (dependencies.isEmpty()) {
			System.out.println("submit pool =>" + newTask.request.getId());
			pool.execute(() -> execute(newTask));
			//execute(newTask);
		} else {
			System.out.println("submit cf => " + newTask.request.getId());
			after(dependencies).thenRun(() -> {

				execute(newTask);
			});
		}
	}
	
	private void addTaskMap(Task newTask) {
		scheduledMap.put(newTask.request.getId(), newTask);
	}
	
	private static CompletableFuture<Void> after(List<CompletableFuture<Void>> fs) {
		if (fs.size() == 1)
			return fs.get(0); // fast path
		return CompletableFuture.allOf(fs.toArray(new CompletableFuture[0]));
	}
	
    private void execute(Task task) {
    	delay.ensureMinCost(() -> {
    		System.out.println(task.request.getId());
    		flagLastExecuted.set(task.request.getId());
    		return new String();
		});
    }

    
	@Test
	public void testSequentialWorkflow() {
		delay = new SyntacticDelay(100000000);
		// Recovery from log
		Stopwatch stopwatch = Stopwatch.createStarted();
		for (int i = 0; i < logRequests.length; i++) {
			byte[] bs = logRequests[i];
			KvsCmd cmdFromLog = Utils.byteArrayToCmd(bs);
			Task newTask = new Task(cmdFromLog);
			execute(newTask);
		}
        stopwatch.stop();
        //System.out.println("Time elapsed: "+ stopwatch.elapsed(TimeUnit.MILLISECONDS));
        System.out.println("Recovery time elapsed: "+ stopwatch.elapsed(TimeUnit.MILLISECONDS));
        System.out.println("Recovery time elapsed: "+ stopwatch.elapsed(TimeUnit.SECONDS));
        //System.out.println("Time elapsed: "+ stopwatch.elapsed(TimeUnit.SECONDS));
	}
	
	
	@Test
	public void testParallelWorkflow() {
		Stopwatch stopwatch = Stopwatch.createStarted();
		// Recovery from log
		for (int i = 0; i < logRequests.length; i++) {
			
			if (i == 0) {
				delay = new SyntacticDelay(2000000000);
			} else {
				delay = new SyntacticDelay(100000000);
			}
			byte[] bs = logRequests[i];
			KvsCmd cmdFromLog = Utils.byteArrayToCmd(bs);

			Task newTask = new Task(cmdFromLog);
			addTaskMap(newTask);
			List<CompletableFuture<Void>> dependencies = new LinkedList<>();
			
			if (newTask.request.getDependencies() != null && !newTask.request.getDependencies().isEmpty()) {
				Task t = null;
				for (KvsCmd cmdDependency : newTask.request.getDependencies()) {
					if (scheduledMap.containsKey(cmdDependency.getId())) {
						t = scheduledMap.get(cmdDependency.getId());
					} else {
						Task depTask = new Task(cmdDependency);
						scheduledMap.put(cmdDependency.getId(), depTask);
						t = depTask;
					}
					if (!t.future.isDone()) {
						dependencies.add(t.future);
					}
					
					// se ele nao estiver no map pode ser um erro, pode ser um comando de outro batch/checkpoint
					// isso deixaria o comando orfao, como temos o comando, podemos forçar a execuçao dele
					// O problema 'e que se o comando foi executado, ele sera executado de novo.
				}
			}
			
			submit(newTask, dependencies);

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
        //System.out.println("Time elapsed: "+ stopwatch.elapsed(TimeUnit.MILLISECONDS));
        System.out.println("Recovery time elapsed: "+ stopwatch.elapsed(TimeUnit.MILLISECONDS));
        System.out.println("Recovery time elapsed: "+ stopwatch.elapsed(TimeUnit.SECONDS));
        //System.out.println("Time elapsed: "+ stopwatch.elapsed(TimeUnit.SECONDS));
        System.out.println("flag last command executed: "+ flagLastExecuted.get());
	}

	private byte[][] generateCommands(int n) {
		ThreadLocalRandom random = ThreadLocalRandom.current();
		int maxKey = 1000;
		float sparseness = 10.0f;
		float conflict = 1.0f;
		byte[][] logRequests = new byte[n][];
		KvsCmd cmdDep = null;

		for (int i = 0; i < n; i++) {
			// Generating the cmd without dependencies
			KvsCmd cmd = Utils.generateRandomCmd(random, maxKey, sparseness, conflict);
			cmd.setId((long) i);
			
			byte[] cmdInBytes = Utils.cmdToByteArray(cmd);
			//Utils.printBytes(cmdInBytes);
			// Now lets add dependencies in cmd
			// but first, lets convert it to java object
			KvsCmd cmdFromBytes = Utils.byteArrayToCmd(cmdInBytes);
			//System.out.println("=> cmdFromBytes: " + cmdFromBytes.toString());
			if (i == 0) {
				cmdDep = cmd;
			} else if (i > 0) {
				int randomOp = random.nextInt(2);

				if (randomOp == 0) {
					ArrayList<KvsCmd> dependencies = new ArrayList<KvsCmd>(1);
					dependencies.add(cmdDep);
					cmdFromBytes.setDependencies(dependencies);
				}
			}
			System.out.println("==>>" + cmdFromBytes);
			cmdInBytes = Utils.cmdToByteArray(cmdFromBytes);
			//Utils.printBytes(cmdInBytes);
			logRequests[i] = cmdInBytes;
		}
		return logRequests;
	}
}
