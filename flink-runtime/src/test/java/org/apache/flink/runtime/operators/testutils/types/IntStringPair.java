package org.apache.flink.runtime.operators.testutils.types;

/**
 * Created by lungao on 9/25/2015.
 */
public class IntStringPair implements Comparable<IntStringPair>{
	
	private int key;
	private String value;
	
	public IntStringPair(){}
	
	public IntStringPair(int key, String value) {
		this.key = key;
		this.value = value;
	}
	
	public int getKey(){
		return this.key;
	}
	
	public void setKey(int key) {
		this.key = key;
	}
	
	public String getValue(){
		return this.value;
	}
	
	public void setValue(String value) {
		this.value = value;
	}
	
	@Override
	public String toString() {
		return "(" + this.key + "/" + this.value + ")";
	}

	@Override
	public int compareTo(IntStringPair o) {
		return this.getValue().compareTo(o.getValue()) > 0 ? 1 : -1;
	}
}
