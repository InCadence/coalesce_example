/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.incadencecorp.coalesce.ingester.gdelt;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

import com.incadencecorp.coalesce.common.exceptions.CoalescePersistorException;
import com.incadencecorp.coalesce.framework.persistance.ServerConn;
import com.incadencecorp.coalesce.framework.persistance.accumulo.AccumuloDataConnector;

/**
 * Reads all data between two rows; all data after a given row; or all data in a table, depending on the number of arguments given.
 */
public class ReadData {
  public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    if (args.length < 5 || args.length > 7) {
      System.out
          .println("bin/accumulo accumulo.examples.helloworld.ReadData <instance name> <zoo keepers> <username> <password> <tablename> [startkey [endkey]]");
      System.exit(1);
    }
    
    String dbName = "bdp";
	String zookeepers = "10.10.10.74";
	String user = "root";
	String password = "accumulo";
	ServerConn serverConnection = new ServerConn.Builder().db(dbName).serverName(zookeepers).user(user).password(password).build();
	AccumuloDataConnector accumuloDataConnector;
	try {
		accumuloDataConnector = new AccumuloDataConnector(serverConnection);
		Connector dbConnector = accumuloDataConnector.getDBConnector();
		
		BatchScanner scanner = dbConnector.createBatchScanner(args[4], new Authorizations().EMPTY, 4);
		
		ArrayList<Range> ranges = new ArrayList<>();
		Range testRange = new Range();
		ranges.add(testRange);
		scanner.setRanges(ranges);
		
		Iterator<Entry<Key,Value>> iter = scanner.iterator();
		
		while(iter.hasNext())
		{
			/*
			Entry<Key,Value> e = iter.next();
			Text colf = e.getKey().getColumnFamily();
			Text colq = e.getKey().getColumnQualifier();
			System.out.println("row: " + e.getKey().getRow() + "\ncolf: " + colf + "\ncolq: " + colq);
			System.out.println("OTHER STUFF "+e.getKey().toString());
			System.out.println("value: " + e.getValue().getClass().getName() + "\n" + e.getValue().toString()+"\n\n");
			*/
		}
    
	    /*String instanceName = args[0];
	    String zooKeepers = args[1];
	    String user = args[2];
	    byte[] pass = args[3].getBytes();
	    String tableName = args[4];
	    
	    ZooKeeperInstance instance = new ZooKeeperInstance(instanceName, zooKeepers);
	    Connector connector = instance.getConnector(user, pass);
	    
	    Scanner scan = connector.createScanner(tableName, Constants.NO_AUTHS);
	    Key start = null;
	    if (args.length > 5)
	      start = new Key(new Text(args[5]));
	    Key end = null;
	    if (args.length > 6)
	      end = new Key(new Text(args[6]));
	    scan.setRange(new Range(start, end));
	    Iterator<Entry<Key,Value>> iter = scan.iterator();
	    
	    while (iter.hasNext()) {
	      Entry<Key,Value> e = iter.next();
	      Text colf = e.getKey().getColumnFamily();
	      Text colq = e.getKey().getColumnQualifier();
	      System.out.println("row: " + e.getKey().getRow() + "\ncolf: " + colf + "\ncolq: " + colq);
	      System.out.println("OTHER STUFF "+e.getKey().toString());
	      System.out.println("value: " + e.getValue().getClass().getName() + "\n" + e.getValue().toString()+"\n\n");
	    }*/
    
		accumuloDataConnector.close();
	} catch (CoalescePersistorException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
  }
}