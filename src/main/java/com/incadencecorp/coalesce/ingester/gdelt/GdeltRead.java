package com.incadencecorp.coalesce.ingester.gdelt;

import java.io.IOException;
import java.sql.SQLException;

import javax.sql.rowset.CachedRowSet;

import org.codehaus.jettison.json.JSONException;
import org.geotools.data.Query;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.opengis.filter.Filter;

import com.incadencecorp.coalesce.common.exceptions.CoalesceException;
import com.incadencecorp.coalesce.common.exceptions.CoalescePersistorException;
import com.incadencecorp.coalesce.framework.CoalesceFramework;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceEntity;
import com.incadencecorp.coalesce.framework.persistance.ServerConn;
import com.incadencecorp.coalesce.framework.persistance.accumulo.AccumuloPersistor;
import com.incadencecorp.coalesce.framework.persistance.postgres.PostGreSQLPersistor;
import com.incadencecorp.coalesce.framework.persistance.postgres.PostGreSQLPersistorExt2;
import com.incadencecorp.coalesce.search.resultset.*;
import com.incadencecorp.oe.ingest.gdelt.artifact.GDELTArtifact;


public class GdeltRead {
	
    
    private static void coalSearch(ServerConn conn) 
    		throws CQLException, SQLException, CoalesceException, JSONException {
    	
 
        //AccumuloPersistor persistor = new AccumuloPersistor(conn);
        PostGreSQLPersistorExt2 persistor = new PostGreSQLPersistorExt2();
        persistor.setConnectionSettings(conn);
        persistor.setSchema("coalesce");
        CoalesceFramework coalesceFramework = new CoalesceFramework();
    	coalesceFramework.setAuthoritativePersistor(persistor);	
    	
    	// Postgres will not accept no filter which it should
    	// But we need a some filter to work.
    	Filter filter = CQL.toFilter("not ("+GDELTArtifact.getQueryName()+".SourceFileName = '')");

        
    	Query query = new Query();
    	query.setFilter(filter);

    	query.setStartIndex(0);
    	System.out.println("Running query");
		long qstartTime = System.currentTimeMillis();
		CachedRowSet rowset = persistor.search(query).getResults();
		long qstopTime = System.currentTimeMillis();
		System.out.println("Query Elapsed time was " + (qstopTime - qstartTime)/1000.0 + " seconds.");
		
		//rowset.getArray(persistor.ENTITY_KEY_COLUMN_NAME);    // this FAILS with  java.sql.SQLException: Invalid cursor position
		
		String[] objectIDs = new String[rowset.size()];
		int row = 0;
		while (rowset.next()) {
			objectIDs[row] = rowset.getString("objectKey");
		    ++row;
		}
		
		// Get the Entities
		System.out.println("Getting " + rowset.size() + " entities");
		long startTime = System.currentTimeMillis();
		CoalesceEntity[] ceArr = coalesceFramework.getCoalesceEntities(objectIDs);
		long stopTime = System.currentTimeMillis();
		
		System.out.println("Elapsed time was " + (stopTime - startTime)/1000.0 + " seconds.");
    }    
    
    
    public static void main(String[] args) 
    		throws CQLException, IOException, SQLException, CoalesceException, 
    		JSONException, CoalescePersistorException {
    	
    	String dbName = "coalesce";
	   	String zookeepers = "accumulodev";
	   	String user = "enterprisedb";
	   	String password = "enterprisedb";
	   	ServerConn conn = new ServerConn.Builder().db(dbName).serverName(zookeepers).user(user).password(password).build();
	   	/* try {
			Thread.sleep(20000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} */
    	conn.setUser(user);
        conn.setPassword(password);

	   	while (true) {
	   		coalSearch(conn);
	   		try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	   	}
//		System.out.println("DONE");
    	
    }
}
