package pucrs.antunes.causalLog;

import static org.junit.Assert.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

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

		assertTrue(future1.isDone());
		assertTrue(future2.isDone());
		assertTrue(future3.isDone());
	}

}
