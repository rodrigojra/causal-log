package pucrs.antunes.causalLog;

import java.util.ArrayList;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import pucrs.antunes.causalLog.recovery.map.KvsCmd;
import pucrs.antunes.causalLog.recovery.model.CreateDependencyTree;
import pucrs.antunes.causalLog.recovery.model.DependenciesAttached;
import pucrs.antunes.causalLog.recovery.model.Sequential;
import pucrs.antunes.causalLog.utils.Utils;

/**
 * This is a causality for recovery log simulation
 * 
 * @author Rodrigo Antunes
 *
 */
public class CausalLogRecoveryApp {

	public static void main(String[] args) {

		System.out.println("Causal logging");
		Options options = new Options();
		options.addOption(Option.builder("g").longOpt("generate-log").hasArg(true)
				.desc("workload size ([REQUIRED] or use --workload)" + "\nsparseness ([REQUIRED] or use --sparseness) ")
				.numberOfArgs(2).required(false).build());
		options.addOption(
				Option.builder("r").longOpt("run-model").hasArg(true)
						.desc("recovery model ([REQUIRED] sequential , graph, attached)"
								+ "\n number of threads ([REQUIRED] 1, 2, 4, 8)")
						.numberOfArgs(2).required(false).build());

		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = null;
		try {

			for (String arg : args) {
				System.out.println("We have = " + arg);
			}
			cmd = parser.parse(options, args);

			if (cmd.hasOption("g")) {
				String[] parameters = cmd.getOptionValues("g");
				/*
				 * for (String parameter : parameters) { System.out.println("We have = " +
				 * parameter); }
				 */
				int workloadSize = Integer.parseInt(parameters[0]);
				float sparseness = Float.parseFloat(parameters[1]);

				System.out.println("Generating Recovery Log with workload size = " + workloadSize + " and sparseness = "
						+ sparseness);
				generateRecoveryLog(workloadSize, sparseness);
				System.out.println("Finished generation of Recovery Log ");

			} else if (cmd.hasOption("r")) {
				String interactionId = cmd.getOptionValue("r");
				System.out.println("We have --interactionId option " + interactionId);
				if (cmd.hasOption("c")) {
					System.out.println("( --clientId option is omitted because --interactionId option is defined)");
				}
			} else {
				System.out.println("please specify one of the command line options: ");
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("Causal recovery log simulation", options);
			}
		} catch (ParseException pe) {
			System.out.println("Error parsing command-line arguments!");
			System.out.println("Please, follow the instructions below:");
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("Log messages to sequence diagrams converter", options);
			System.exit(1);
		}

	}

	private static void generateRecoveryLog(int workloadSize, float sparseness) {
		int maxKey = 5, conflict = 1;
		System.out.println("Generating commands...");
		Utils.generateRecoveryLog(workloadSize, maxKey, sparseness, conflict);
		System.out.println("Generating dependencies...");
		// Utils.generateDependenciesForEachCmd(cmdArray);
		System.out.println("Generating simulated recovery log...");
	}

	private static void runWorkload(int threads) {
		ArrayList<KvsCmd> readRecoveryFromFile = Utils.readRecoveryLogFromFile();
		KvsCmd[] cmdArray = readRecoveryFromFile.toArray(new KvsCmd[0]);
		byte[][] recoveryLog = Utils.convertCmdArrayToBytes(cmdArray, readRecoveryFromFile.size());
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
