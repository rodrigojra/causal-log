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

	public static KvsCmd generateRandomCmd(Random random, int maxKey, float sparseness, float conflict) {
		KvsCmdType cmdType;
		cmdType = generateOperationType(random);

		int key, mid = maxKey / 2;
		do {
			key = (int) Math.round(random.nextGaussian() * (mid * sparseness) + mid);
		} while (key < 0 || key >= maxKey);

		KvsCmd appCmd = new KvsCmd(cmdType, key, random.nextInt());

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
		//TODO: mudar para ||??
		return (cmdA.getType().isWrite || cmdB.getType().isWrite) && cmdA.getKey().equals(cmdB.getKey());
	}

	public static void generateRecoveryLog(int workloadSize, int maxKey, float sparseness, float conflict) {
		ArrayList<KvsCmd> cmdArray = new ArrayList<KvsCmd>();
		generateCommands(workloadSize, maxKey, sparseness, conflict, cmdArray);
		generateDependenciesForEachCmd(cmdArray);
		saveCmdToFile(cmdArray);
		saveCmdToJSON(cmdArray);

	}

	public static void saveCmdToJSON(ArrayList<KvsCmd> cmdArray) {
		Gson gson = new Gson();
		try {
			Date date = new Date();
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SS");
			Writer writer = new FileWriter("target/recovery-json-" + dateFormat.format(date) + ".json");
			// convert users list to JSON file
			gson.toJson(cmdArray, writer);

			// close writer
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void saveCmdToFile(ArrayList<KvsCmd> cmdArray) {
		try {
			Date date = new Date();
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SS");

			//FileOutputStream writeData = new FileOutputStream("target/recovery-bin" + dateFormat.format(date) + ".dat");
			FileOutputStream writeData = new FileOutputStream("target/recovery-bin.dat");
			ObjectOutputStream writeStream = new ObjectOutputStream(writeData);

			writeStream.writeObject(cmdArray);
			writeStream.flush();
			writeStream.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static ArrayList<KvsCmd> readRecoveryLogFromFile() {
		ArrayList<KvsCmd> cmdArray = null;
		try {
			FileInputStream readData = new FileInputStream("target/recovery-bin.dat");
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
			// Generating the cmd without dependencies
			KvsCmd cmd = generateRandomCmd(random, maxKey, sparseness, conflict);
			cmd.setId((long) i);
			cmdArray.add(cmd);
		}
	}

	private static void generateDependenciesForEachCmd(ArrayList<KvsCmd> cmdArrayList) {
		
		//dependenciesOneLevel(cmdArrayList);

		//ArrayList<KvsCmd> dependencyList = null;

		for (int index = cmdArrayList.size() - 1; index >= 0; index--) {
			KvsCmd cmd = cmdArrayList.get(index);
			//dependencyList = new ArrayList<KvsCmd>(0);

			//for (KvsCmd dep : cmdArrayList) {
			for (int j = index -1; j >= 0; j--) {
//				if (j < 0) {
//					break;
//				}
				KvsCmd dep = cmdArrayList.get(j);
				
				if (conflictWith(cmd, dep)) {
					cmd.getDependencies().add(dep);
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
			
			int next =  index - 1;
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
