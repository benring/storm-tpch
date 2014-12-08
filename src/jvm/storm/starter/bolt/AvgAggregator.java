package storm.starter.bolt;
import java.util.List;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class AvgAggregator  extends SelectBolt {

    int 	count = 0;
    int		sum = 0;

    public AvgAggregator (List<String> schema, String _field, String asLabel) {
    	super(schema, _field, asLabel);
    }

    public AvgAggregator (List<String> schema, String _field) {
    	this(schema, _field, _field);
    }

    @Override
    public void execute(Tuple tuple) {
    	int elm = Integer.parseInt(tuple.getString(col));
    	count++;
    	sum += elm;
	    _collector.emit(new Values(label, ((float) sum) / count));
	    _collector.ack(tuple);
    }

}