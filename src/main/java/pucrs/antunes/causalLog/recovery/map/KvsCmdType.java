/**
 * 
 */
package pucrs.antunes.causalLog.recovery.map;

import java.util.Map;

/**
 * @author Rodrigo Antunes
 *
 */
public enum KvsCmdType {
	PUT(true) {
		@Override
		public
		Integer execute(Map<Integer, Integer> state, KvsCmd cmd) {
			Integer key = cmd.getKey();
			Integer value = cmd.getValue();
			state.put(key, value);
			return value;
		}
	},

	GET(false) {
		@Override
		public
		Integer execute(Map<Integer, Integer> state, KvsCmd cmd) {
			return state.get(cmd.getKey());
		}
	},

	REMOVE(true) {
		@Override
		public
		Integer execute(Map<Integer, Integer> state, KvsCmd cmd) {
			return state.get(cmd.getKey());
		}
	};

	public boolean isWrite;

	KvsCmdType(boolean isWrite) {
		this.isWrite = isWrite;
	}

	public abstract Integer execute(Map<Integer, Integer> state, KvsCmd cmd);
}