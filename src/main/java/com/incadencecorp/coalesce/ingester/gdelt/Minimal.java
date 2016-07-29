package com.incadencecorp.coalesce.ingester.gdelt;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.geotools.data.DataStoreFinder;
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore;

public class Minimal {

	public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, IOException {
		System.out.println("Start");
		Map<String, String> dsConf = new HashMap<String, String>();
		dsConf.put("instanceId", "coalesce");
		dsConf.put("zookeepers", "localhost:2181");
		dsConf.put("user", "root");
		dsConf.put("password", "rootroot");
		dsConf.put("tableName", "CoalesceSearch");
		dsConf.put("auths", ""); // Auths will be empty for now
		AccumuloDataStore dataStore = (AccumuloDataStore) DataStoreFinder.getDataStore(dsConf);
		dataStore.dispose();
		System.out.println("Finish");

	}

}
