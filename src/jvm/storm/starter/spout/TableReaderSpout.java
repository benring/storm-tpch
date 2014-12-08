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
package storm.starter.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
//import backtype.storm.utils.Utils;

import java.io.BufferedReader;
//import java.io.File;
//import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
//import java.util.Scanner;
//import java.util.ArrayList;


public class TableReaderSpout extends BaseRichSpout {
  SpoutOutputCollector _collector;
  Random _rand;
  BufferedReader source;
  String [] fields;
  String line;
  String filename;
  String delimiter;

  
  public TableReaderSpout (String _filename, String [] _fields, String _delim) {
	  this.filename = _filename;
	  this.fields = _fields;
	  this.line = "row";
	  this.delimiter = _delim;
  }

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
    
    try {
		source = new BufferedReader(new FileReader(this.filename));
		source.mark(0);
	} catch (IOException e) {
    	e.printStackTrace();
	}
  }

  @Override
  public void nextTuple() {
	  
    try {
    	String line = source.readLine();
    	if (line != null) {
        	String [] elms = line.split(delimiter);
            _collector.emit(new Values(elms));
    	}
    }
    catch (Exception e) {
    	e.printStackTrace();
    }
  }

  @Override
  public void ack(Object id) {
  }

  @Override
  public void fail(Object id) {
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(Arrays.asList(fields)));
  }

}