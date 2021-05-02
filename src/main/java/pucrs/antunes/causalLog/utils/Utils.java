package pucrs.antunes.causalLog.utils;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.lang3.SerializationUtils;

import com.google.gson.Gson;

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

	public static boolean conflictWith(KvsCmd cmdA, KvsCmd cmdB) {
		return (cmdA.getType().isWrite || cmdB.getType().isWrite) && cmdA.getKey().equals(cmdB.getKey());
	}

	public static void generateRecoveryLog(int workloadSize, int maxKey, float sparseness, float conflict,
			boolean isJson,  boolean skipDependencies) {
		ArrayList<KvsCmd> cmdArray = new ArrayList<KvsCmd>();
		generateCommands(workloadSize, maxKey, sparseness, conflict, cmdArray);
		
		if (!skipDependencies) {
			generateDependenciesForEachCmd(cmdArray);
		}
		
		saveCmdToFile(cmdArray, workloadSize, sparseness, skipDependencies);
		
		if (isJson) {
			saveCmdToJSON(cmdArray, workloadSize, sparseness);
		}

	}

	public static void saveCmdToJSON(ArrayList<KvsCmd> cmdArray, int workload, float sparseness) {
		Gson gson = new Gson();
		try {
			Writer writer = new FileWriter("target/recovery-json-"+ workload + "-conflict-" + sparseness + ".json");
			// convert users list to JSON file
			gson.toJson(cmdArray, writer);

			// close writer
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void saveCmdToFile(ArrayList<KvsCmd> cmdArray, int workload, float sparseness, boolean skipDependencies) {
		try {
//			Date date = new Date();
//			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SS");

			// FileOutputStream writeData = new FileOutputStream("target/recovery-bin" +
			// dateFormat.format(date) + ".dat");

			String fileName = "recovery/recovery-w-" + workload + "-conflict-" + sparseness + ".dat";
			if  (skipDependencies) {
				fileName = "recovery/" + workload +"-log-without-conflict.dat";
			}
			FileOutputStream writeData = new FileOutputStream(fileName);
			ObjectOutputStream writeStream = new ObjectOutputStream(writeData);

			writeStream.writeObject(cmdArray);
			writeStream.flush();
			writeStream.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static ArrayList<KvsCmd> readRecoveryLogFromFile(String filename) {
		ArrayList<KvsCmd> cmdArray = null;
		try {
			FileInputStream readData = new FileInputStream("recovery/" + filename);
			ObjectInputStream readStream = new ObjectInputStream(readData);
			cmdArray = (ArrayList<KvsCmd>) readStream.readObject();
			readStream.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return cmdArray;
	}

	private static void generateCommands(int workloadSize, int maxKey, float sparseness, float conflict,
			ArrayList<KvsCmd> cmdArray) {
		ThreadLocalRandom random = ThreadLocalRandom.current();

		for (int i = 0; i < workloadSize; i++) {
			//KvsCmd cmd = generateRandomCmd(random, maxKey, sparseness, conflict);
			KvsCmd cmd = newGenerateRandomCmd(random, maxKey, sparseness, conflict);
			
			cmd.setId((long) i);
			cmdArray.add(cmd);
		}
	}

	
	public static KvsCmd newGenerateRandomCmd(Random random, int maxKey, float sparseness, float conflict) {
		
		KvsCmdType cmdType;
		if (random.nextFloat() >= conflict) {
			cmdType = KvsCmdType.GET;
		} else {
			cmdType = KvsCmdType.PUT;
		}

		//int key = nextGaussianKey(random, maxKey, sparseness);
		
		int key = random.nextInt(maxKey);
				

		KvsCmd appCmd = new KvsCmd(cmdType, key, random.nextInt());

		return appCmd;
	}

	private static int nextGaussianKey(Random random, int maxKey, float sparseness) {
		int key, mid = maxKey / 2;
		do {
			key = (int) Math.round(random.nextGaussian() * (mid * sparseness) + mid);
		} while (key < 0 || key >= maxKey);
		return key;
	}
	
	public static KvsCmd generateRandomCmd(Random random, int maxKey, float sparseness, float conflict) {
		
		KvsCmdType cmdType;
		if (random.nextFloat() >= conflict) {
			cmdType = KvsCmdType.GET;
		} else {
			cmdType = KvsCmdType.PUT;
		}

		int key = nextGaussianKey(random, maxKey, sparseness);

		KvsCmd appCmd = new KvsCmd(cmdType, key, random.nextInt());

		return appCmd;
	}

	private static void generateDependenciesForEachCmd(ArrayList<KvsCmd> cmdArrayList) {
		for (int index = cmdArrayList.size() - 1; index >= 0; index--) {
			KvsCmd cmd = cmdArrayList.get(index);

			for (int j = index - 1; j >= 0; j--) {
				KvsCmd dep = cmdArrayList.get(j);

				if (conflictWith(cmd, dep)) {
					KvsCmd depCmd = new KvsCmd(dep.getType(), dep.getKey(), dep.getValue());
					depCmd.setId(dep.getId());
					depCmd.setDependencies(null);
					cmd.getDependencies().add(depCmd);
				}
			}
		}

//		for (int i = cmdArray.length - 1; i >= 0; i--) {
//			dependencyList = new ArrayList<KvsCmd>();
//			KvsCmd cmd = cmdArray[i];
//
//			for (int j = 0; j < cmdArray.length; j++) {
//				KvsCmd dep = cmdArray[j];
//
//				if (cmd.getId() == dep.getId()) {
//					// In this case we don't want to generate a dependency as the command is the
//					// same.
//					continue;
//				}
//
//				if (conflictWith(cmd, dep)) {
//					dependencyList.add(dep);
//				}
//			}
//			cmd.setDependencies(dependencyList);
//		}
	}

	private static void dependenciesOneLevel(ArrayList<KvsCmd> cmdArrayList) {
		for (int index = cmdArrayList.size() - 1; index >= 0; index--) {

			int next = index - 1;
			if (next < 0) {
				break;
			}

			KvsCmd cmd = cmdArrayList.get(index);
			KvsCmd dep = cmdArrayList.get(next);

			if (conflictWith(cmd, dep)) {
				cmd.getDependencies().add(dep);
			}

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
