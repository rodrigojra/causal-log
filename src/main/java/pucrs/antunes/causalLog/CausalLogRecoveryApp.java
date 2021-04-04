package pucrs.antunes.causalLog;

import org.apache.log4j.Logger;

import pucrs.antunes.causalLog.recovery.map.KvsCmd;
import pucrs.antunes.causalLog.recovery.model.CreateDependencyTree;
import pucrs.antunes.causalLog.recovery.model.DependenciesAttached;
import pucrs.antunes.causalLog.recovery.model.Sequential;
import pucrs.antunes.causalLog.utils.Utils;

/**
 * Hello world!
 *
 */
public class CausalLogRecoveryApp {
	
	private static final Logger LOGGER = Logger.getLogger(CausalLogRecoveryApp.class);
	
	public static void main(String[] args) {

		System.out.println("Causal logging");

		// recovery type
		// workload size
		// threads
		// sparseness
		if (args.length != 3) {
			System.out
					.println("Usage: App " + "<recovery model> " + "<workload size> " + "<threads> " + "<sparseness> ");
			System.exit(1);
		}
		
		LOGGER.info("recovery type " + args[0]);
		LOGGER.info("workload size " + args[1]);
		LOGGER.info("threads " + args[2]);
		LOGGER.info("sparseness " + args[3]);
		
		runWorkload(
				Integer.parseInt(args[0]), 
				Integer.parseInt(args[1]), 
				Integer.parseInt(args[2]),
				Integer.parseInt(args[3]));
	}

	private static void runWorkload(int recoveryModel, int workloadSize, int threads, int sparseness) {
		int maxKey = 0, conflict = 0;
		
		KvsCmd[] cmdArray = Utils.generateCommands(workloadSize, maxKey, sparseness, conflict);
		
		byte[][] recoveryLog = Utils.convertCmdArrayToBytes(cmdArray, workloadSize);
		
		// recoveryModel: 0 = Sequential, 1 = CreateDependencyTree, 2 = DependenciesAttached
		switch (recoveryModel) {
		case 0:
			Sequential sequencial = new Sequential(recoveryLog);
			sequencial.executeWorkflow();
			break;

		case 1:
			CreateDependencyTree createDependencyTree = new CreateDependencyTree(recoveryLog);
			createDependencyTree.executeWorkflow();
			break;
			
		case 2:
			DependenciesAttached dependenciesAttached = new DependenciesAttached(recoveryLog);
			break;
		default:
			LOGGER.error("Invalid recovery type");
			break;
		}
	}
}
