package pucrs.antunes.causalLog;

import java.util.ArrayList;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;

import pucrs.antunes.causalLog.recovery.map.KvsCmd;
import pucrs.antunes.causalLog.recovery.model.CreateDependencyTree;
import pucrs.antunes.causalLog.recovery.model.DependenciesAttached;
import pucrs.antunes.causalLog.recovery.model.RecoveryModel.Models;
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
		options.addOption(Option
				.builder("g").longOpt("generate-log").hasArg(true).desc("workload size ([REQUIRED] or use --workload)"
						+ "\nsparseness ([REQUIRED] or use --sparseness) " + "\njson file")
				.numberOfArgs(4).required(false).build());
		options.addOption(Option.builder("r").longOpt("run-model").hasArg(true)
				.desc("recovery model ([REQUIRED] sequential , graph, attached)"
						+ "\n number of threads ([REQUIRED] 1, 2, 4, 8)" + "\n filename")
				.numberOfArgs(4).required(false).build());
//		options.addOption(Option.builder("s").longOpt("skip-dependencies").hasArg(false)
//				.desc("skip-dependencies")
//				.build());
//		options.addOption(Option.builder("t").longOpt("time").hasArg(true)
//				.desc("time in nanoseconds")
//				.numberOfArgs(1).required(false).build());

		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = null;
		int delayTime = 1000000;

		try {
			cmd = parser.parse(options, args);

//			if (cmd.hasOption("s")) {
//				skipDependencies = true;
//				System.out.println("skiping dependencies");
//			}
//
//			if (cmd.hasOption("t")) {
//				String[] parameters = cmd.getOptionValues("t");
//				delayTime = Integer.parseInt(parameters[0]);
//				System.out.println("delay time "+delayTime);
//			}	

			if (cmd.hasOption("g")) {
				String[] parameters = cmd.getOptionValues("g");
				int workloadSize = Integer.parseInt(parameters[0]);
				float sparseness = Float.parseFloat(parameters[1]);
				boolean isJson = Boolean.parseBoolean(parameters[2]);
				boolean skipDependencies = Boolean.parseBoolean(parameters[3]);

				System.out.println("Generating Recovery Log with workload size = " + workloadSize + " and sparseness = "
						+ sparseness);
				generateRecoveryLog(workloadSize, sparseness, isJson, skipDependencies);
				System.out.println("Finished generation of Recovery Log ");

			} else if (cmd.hasOption("r")) {
				String[] parameters = cmd.getOptionValues("r");
				String model = StringUtils.upperCase(parameters[0]);
				int threads = Integer.parseInt(parameters[1]);
				String filename = parameters[2];
				delayTime = Integer.parseInt(parameters[3]);
				runWorkload(model, threads, filename, delayTime);
				System.out.println("Finished execution of model " + model + " with " + threads + " threads ");
			} else {
				System.out.println("Please specify one of the command line options: ");
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

	private static void generateRecoveryLog(int workloadSize, float sparseness, boolean isJson, boolean skipDependencies) {
		int maxKey = workloadSize, conflict = 1;
		System.out.println("Generating simulated recovery log...");
		Utils.generateRecoveryLog(workloadSize, maxKey, sparseness, conflict, isJson, skipDependencies);
	}

	private static void runWorkload(String model, int threads, String filename, int delayTime) {
		ArrayList<KvsCmd> readRecoveryFromFile = Utils.readRecoveryLogFromFile(filename);

		if (StringUtils.containsIgnoreCase(model, Models.SEQUENTIAL.toString())) {
			Sequential sequencial = new Sequential(readRecoveryFromFile, threads, delayTime);
			sequencial.executeWorkflow();
		} else if (StringUtils.containsIgnoreCase(model, Models.GRAPH.toString())) {
			CreateDependencyTree createDependencyTree = new CreateDependencyTree(readRecoveryFromFile, threads, delayTime);
			createDependencyTree.executeWorkflow();
		} else if (StringUtils.containsIgnoreCase(model, Models.ATTACHED.toString())) {
			DependenciesAttached dependenciesAttached = new DependenciesAttached(readRecoveryFromFile, threads, delayTime);
			dependenciesAttached.executeWorkflow();
		}
	}

}
