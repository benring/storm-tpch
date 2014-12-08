/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
//import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Arrays;
import java.util.Map;

import storm.starter.bolt.SelectBolt;
import storm.starter.bolt.SumAggregator;
import storm.starter.bolt.WhereBolt;
import storm.starter.spout.TableReaderSpout;
import storm.starter.spout.TpchSchema;
import storm.starter.sql.DateComparer;
import storm.starter.sql.IntComparer;
import storm.starter.sql.SQLComparer;


/**
 * This is a basic example of a Storm topology.
 */
public class SelectionTopology {



  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();
    
    TableReaderSpout lineitem = new TableReaderSpout("lineitem200", TpchSchema.lineitem, "\\|");
    
    WhereBolt linenum = new WhereBolt(TpchSchema.lineitem,"l_quantity", new IntComparer(40, SQLComparer.GREATER));
    WhereBolt shipdate = new WhereBolt(TpchSchema.lineitem,"l_shipdate", new DateComparer("1998-12-01", SQLComparer.LESSER));

    
    SelectBolt l_quantity = new SelectBolt(Arrays.asList(TpchSchema.lineitem), "l_quantity");

    SumAggregator sum_qty = new SumAggregator(Arrays.asList(TpchSchema.lineitem), "l_quantity");
    
   
    builder.setSpout("row", lineitem, 2);
    builder.setBolt("SHIP-DATE", shipdate, 2).shuffleGrouping("row");
//    builder.setBolt("QUANTITY",  l_quantity, 2).shuffleGrouping("SHIP-DATE");
    builder.setBolt("sum_qty", sum_qty, 2).shuffleGrouping("SHIP-DATE");
    //builder.setBolt("count_order", )


    Config conf = new Config();
    conf.setDebug(true);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }
    else {

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("TPCH_in_Storm", conf, builder.createTopology());
      Utils.sleep(10000);
      cluster.killTopology("TPCH_in_Storm");
      cluster.shutdown();
    }
  }
}
