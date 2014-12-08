package storm.starter.sql;

public abstract interface SQLComparer<T> {

	static int GREATER = 1;
	static int LESSER = -1;
	static int EQUALS = 0;
	
	abstract boolean Compare (T x);
	
	abstract T Parse (String s);
	
}
