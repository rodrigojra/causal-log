/**
 * 
 */
package pucrs.antunes.causalLog.recovery.map;

import java.io.Serializable;
import java.util.List;

/**
 * @author rodrigo
 *
 */
public class KvsCmd implements Serializable {

	private static final long serialVersionUID = 5802411485930840619L;
	
	public KvsCmd(KvsCmdType cmdType, Integer key, Integer value, List<KvsCmd> dependencies) {
		super();
		this.type = cmdType;
		this.key = key;
		this.value = value;
		this.dependencies = dependencies;
	}

	private Long id;
	private KvsCmdType type;
	private Integer key;
	private Integer value;
	private List<KvsCmd> dependencies;

	
	public KvsCmdType getType() {
		return type;
	}
	
	public void setType(KvsCmdType cmdType) {
		this.type = cmdType;
	}
	
	public Integer getKey() {
		return key;
	}
	
	public void setKey(Integer key) {
		this.key = key;
	}
	public Integer getValue() {
		return value;
	}
	
	public void setValue(Integer value) {
		this.value = value;
	}
	
	public List<KvsCmd> getDependencies() {
		return dependencies;
	}
	
	public void setDependencies(List<KvsCmd> dependencies) {
		this.dependencies = dependencies;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	@Override
	public String toString() {
		return "AppCmd [id=" + id + ", cmdType=" + type + ", key=" + key + ", value=" + value + ", dependencies="
				+ dependencies + "]";
	}
}
