package pucrs.antunes.causalLog;

import static org.junit.Assert.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class CFTest {

	@Test
	public void test() {
		CompletableFuture<String> future1 = CompletableFuture.supplyAsync(() -> "Hello");
		CompletableFuture<String> future2 = CompletableFuture.supplyAsync(() -> "Beautiful");
		CompletableFuture<String> future3 = CompletableFuture.supplyAsync(() -> "World");
		
		try {
			System.out.println(future3.get());
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (ExecutionException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(future1, future2, future3);
		
		// ...

		try {
			combinedFuture.get();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		//assertTrue(future1.isDone());
		//assertTrue(future2.isDone());
		//assertTrue(future3.isDone());
		
		ForkJoinPool pool0 = ForkJoinPool.commonPool();
		printPoolDetails(pool0);
		int threads = 2;
		ForkJoinPool pool1 = new ForkJoinPool(threads, ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true, threads,
				threads, 0, null, 60, TimeUnit.SECONDS);
		
		printPoolDetails(pool1);
		
		ForkJoinPool pool2 = ForkJoinPool.commonPool();
		printPoolDetails(pool2);
	}

	private void printPoolDetails(ForkJoinPool pool1) {
		System.out.println("getParallelism()" + pool1.getParallelism());
		System.out.println("getPoolSize()" +pool1.getPoolSize());
		System.out.println("getCommonPoolParallelism()" +pool1.getCommonPoolParallelism());
		System.out.println("getRunningThreadCount()" + pool1.getRunningThreadCount());
		System.out.println();
	}

}
