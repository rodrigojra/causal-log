package pucrs.antunes.causalLog.utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.lang3.SerializationUtils;

import pucrs.antunes.causalLog.recovery.map.KvsCmd;
import pucrs.antunes.causalLog.recovery.map.KvsCmdType;

public class Utils {

	/**
	 * Convert object to byte array
	 * 
	 * @param object
	 * @return
	 */
	public static byte[] cmdToByteArray(Serializable object) {
		return SerializationUtils.serialize(object);
	}

	/**
	 * Convert byte array to object
	 * 
	 * @param bytes
	 * @return
	 */
	public static KvsCmd byteArrayToCmd(byte[] bytes) {
		return SerializationUtils.deserialize(bytes);
	}

	public static void printBytes(byte[] reply) {
		System.out.print("\t[ ");
		for (byte b : reply) {
			System.out.print(", " + b);
		}
		System.out.println(" ]");
	}

	public static KvsCmd generateRandomCmd(Random random, int maxKey, float sparseness, float conflict) {
		KvsCmdType cmdType;
		cmdType = generateOperationType(random);

		int key, mid = maxKey / 2;
		do {
			key = (int) Math.round(random.nextGaussian() * (mid * sparseness) + mid);
		} while (key < 0 || key >= maxKey);

		KvsCmd appCmd = new KvsCmd(cmdType, key, random.nextInt(), null);

		return appCmd;
	}

	private static KvsCmdType generateOperationType(Random random) {
		KvsCmdType requestType;
		int randomOp = random.nextInt(2);

		if (randomOp == 0) {
			requestType = KvsCmdType.GET;
		} else {
			requestType = KvsCmdType.PUT;
		}
		return requestType;
	}

	public static boolean conflictWith(KvsCmd cmdA, KvsCmd cmdB) {
		return (cmdA.getType().isWrite || cmdB.getType().isWrite) && cmdA.getKey().equals(cmdB.getKey());
	}

	public static KvsCmd[] generateCommands(int workloadSize, int maxKey, float sparseness, float conflict) {
		ThreadLocalRandom random = ThreadLocalRandom.current();
		KvsCmd[] cmdArray = new KvsCmd[workloadSize];

		for (int i = 0; i < workloadSize; i++) {
			// Generating the cmd without dependencies
			KvsCmd cmd = generateRandomCmd(random, maxKey, sparseness, conflict);
			cmd.setId((long) i);
			cmdArray[i] = cmd;
		}
		return cmdArray;
	}

	public static void generateDependenciesForEachCmd(KvsCmd[] cmdArray) {

		ArrayList<KvsCmd> dependencyList = null;
		
		for (int i = cmdArray.length - 1; i >= 0; i--) {
			dependencyList = new ArrayList<KvsCmd>();
			KvsCmd cmd = cmdArray[i];

			for (int j = 0; j < cmdArray.length; j++) {
				KvsCmd dep = cmdArray[j];
				
				if (cmd.getId() == dep.getId()) {
					// In this case we don't want to generate a depedency as the command is the same.
					continue;
				}
				
				if (conflictWith(cmd, dep)) {
					dependencyList.add(dep);
				}
			}
			cmd.setDependencies(dependencyList);
		}
	}

	public static byte[][] convertCmdArrayToBytes(KvsCmd[] cmdArray, int workloadSize) {
		byte[][] logRequests = new byte[workloadSize][];

		for (int j = 0; j < cmdArray.length; j++) {
			KvsCmd kvsCmd = cmdArray[j];
			byte[] cmdInBytes = Utils.cmdToByteArray(kvsCmd);
			logRequests[j] = cmdInBytes;
		}
		return logRequests;
	}

}
