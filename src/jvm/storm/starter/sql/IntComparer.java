package storm.starter.sql;

import java.io.Serializable;

public class IntComparer implements SQLComparer<Integer>, Serializable{

	int value;
	int comp;
	
	public IntComparer (int _value, int _comp) {
		this.value = _value;
		this.comp = _comp;
	}
	
	@Override
	public boolean Compare(Integer x) {
		return (Integer.compare(x, value) == comp);
	}

	@Override
	public Integer Parse(String s) {
		return Integer.parseInt(s);
	}
	

}
