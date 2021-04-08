package pucrs.antunes.causalLog;

import pucrs.antunes.causalLog.recovery.map.KvsCmd;
import pucrs.antunes.causalLog.recovery.model.CreateDependencyTree;
import pucrs.antunes.causalLog.recovery.model.DependenciesAttached;
import pucrs.antunes.causalLog.recovery.model.Sequential;
import pucrs.antunes.causalLog.utils.Utils;

/**
 * This is a causality for recovery log simulation
 * @author Rodrigo Antunes
 *
 */
public class CausalLogRecoveryApp {

	public static void main(String[] args) {

		System.out.println("Causal logging");

		// workload size
		// threads
		// sparseness
		if (args.length != 3) {
			System.out
					.println("Usage: App " + "<workload size> " + "<threads> " + "<sparseness> ");
			System.exit(1);
		}

		System.out.println("workload size " + args[0]);
		System.out.println("threads " + args[1]);
		System.out.println("sparseness " + args[2]);

		runWorkload(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]));
	}

	private static void runWorkload(int workloadSize, int threads, int sparseness) {
		int maxKey = 1000, conflict = 1;
		System.out.println("Generating commands..."); 
		KvsCmd[] cmdArray = Utils.generateCommands(workloadSize, maxKey, sparseness, conflict);
		System.out.println("Generating dependencies...");
		Utils.generateDependenciesForEachCmd(cmdArray);
		System.out.println("Generating simulated recovery log...");
		byte[][] recoveryLog = Utils.convertCmdArrayToBytes(cmdArray, workloadSize);

		executeModels(recoveryLog, threads);
	}

	private static void executeModels(byte[][] recoveryLog, int threads) {
		Sequential sequencial = new Sequential(recoveryLog, threads);
		sequencial.executeWorkflow();

		CreateDependencyTree createDependencyTree = new CreateDependencyTree(recoveryLog, threads);
		createDependencyTree.executeWorkflow();

		DependenciesAttached dependenciesAttached = new DependenciesAttached(recoveryLog, threads);
		dependenciesAttached.executeWorkflow();

	}
}
