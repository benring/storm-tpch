package storm.starter.sql;
import java.io.Serializable;
import java.time.LocalDate;

public class DateComparer implements SQLComparer<LocalDate>, Serializable{

	LocalDate value;
	int comp;
	
	public DateComparer (String date, int _comp) {
		this.value = LocalDate.parse(date);
		this.comp = _comp;
	}
	
	@Override
	public boolean Compare(LocalDate x) {
		return (x.compareTo(value) == comp);
	}

	@Override
	public LocalDate Parse(String s) {
		return LocalDate.parse(s);
	}
		

	
}
