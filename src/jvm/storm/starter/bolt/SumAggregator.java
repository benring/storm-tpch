package storm.starter.bolt;
import java.util.List;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SumAggregator  extends SelectBolt {

    int 	sum = 0;

    public SumAggregator (List<String> schema, String _field, String asLabel) {
    	super(schema, _field, asLabel);
    }

    public SumAggregator (List<String> schema, String _field) {
    	this(schema, _field, _field);
    }

    @Override
    public void execute(Tuple tuple) {
    	int elm = Integer.parseInt(tuple.getString(col));
    	sum += elm;
	    _collector.emit(new Values(label, sum));
	    _collector.ack(tuple);
    }

}
