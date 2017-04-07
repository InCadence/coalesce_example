package com.incadencecorp.coalesce.ingester.gdelt;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import javax.sql.rowset.CachedRowSet;

import org.codehaus.jettison.json.JSONException;
import org.geotools.data.Query;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.opengis.filter.Filter;
import org.xml.sax.SAXException;

import com.incadencecorp.coalesce.common.exceptions.CoalesceException;
import com.incadencecorp.coalesce.common.exceptions.CoalescePersistorException;
import com.incadencecorp.coalesce.framework.CoalesceFramework;
import com.incadencecorp.coalesce.framework.CoalesceObjectFactory;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceEntity;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceEntityTemplate;
import com.incadencecorp.coalesce.framework.persistance.ObjectMetaData;
import com.incadencecorp.coalesce.framework.persistance.ServerConn;
import com.incadencecorp.coalesce.framework.persistance.accumulo.AccumuloPersistor;
import com.incadencecorp.coalesce.framework.persistance.postgres.PostGreSQLPersistor;
import com.incadencecorp.coalesce.framework.persistance.postgres.PostGreSQLPersistorExt2;
import com.incadencecorp.coalesce.framework.util.CoalesceTemplateUtil;
import com.incadencecorp.coalesce.search.resultset.*;
import com.incadencecorp.oe.entities.OEAgent;
import com.incadencecorp.oe.entities.OEEntity;
import com.incadencecorp.oe.ingest.gdelt.artifact.GDELTArtifact;


public class GdeltRead {
	
    
    private static void coalSearch(ServerConn conn) 
    		throws CQLException, SQLException, CoalesceException, JSONException {
    	
 
        //AccumuloPersistor persistor = new AccumuloPersistor(conn);
        PostGreSQLPersistorExt2 persistor = new PostGreSQLPersistorExt2();
        persistor.setConnectionSettings(conn);
        //persistor.setSchema("coalesce");
        CoalesceFramework coalesceFramework = new CoalesceFramework();
    	coalesceFramework.setAuthoritativePersistor(persistor);	
    	// Register templates with persister
    	// See if this lets PostGres persister find the context for my field
    	/* This did not help
    	List<ObjectMetaData> templist = persistor.getEntityTemplateMetadata();
    	for (ObjectMetaData templatemd : templist) {
    		String templatexml = persistor.getEntityTemplateXml(templatemd.getKey());
    		CoalesceEntityTemplate template = null;
			try {
				template = CoalesceEntityTemplate.create(templatexml);
			} catch (SAXException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		if (template != null) persistor.registerTemplate(template);
    	}
    	*/
    	// THis did not work either
        CoalesceObjectFactory.register(GDELTArtifact.class);
        CoalesceObjectFactory.register(OEEntity.class);
        CoalesceObjectFactory.register(OEAgent.class);
		
    	CoalesceTemplateUtil.addTemplates(persistor);
    	// Postgres will not accept no filter which it should
    	// But we need some filter to work.
    	
    	// Works in postgres fails in Accumulo
    	Filter filter = CQL.toFilter("not (\"GDELTArtifactRecordset.SourceFileName\" = '')");
    	
    	// Works in accumulo fails in Postgres
//    	Filter filter = CQL.toFilter("not (SourceFileName = '')");
    	
    	String cql = CQL.toCQL(filter);
        
    	Query query = new Query("GDELTArtifactRecordset");
    	query.setFilter(filter);
    	
    	String[] properties = {"GDELTArtifactRecordset.SourceFileName",
                "GDELTArtifactRecordset.globaleventid"};  
    	query.setPropertyNames(properties);
    	

    	query.setStartIndex(0);
    	System.out.println("Running query");
		long qstartTime = System.currentTimeMillis();
		CachedRowSet rowset = persistor.search(query).getResults();
		long qstopTime = System.currentTimeMillis();
		System.out.println("Query Elapsed time was " + (qstopTime - qstartTime)/1000.0 + " seconds.");
		
		//rowset.getArray(persistor.ENTITY_KEY_COLUMN_NAME);    // this FAILS with  java.sql.SQLException: Invalid cursor position
		// Get the Entities
		System.out.println("Getting " + rowset.size() + " entities");
		
		String[] objectIDs = new String[rowset.size()];
		int row = 0;
		while (rowset.next()) {
			objectIDs[row] = rowset.getString("GDELTArtifactRecordset.objectKey");
		    ++row;
		}
		
		long startTime = System.currentTimeMillis();
		CoalesceEntity[] ceArr = coalesceFramework.getCoalesceEntities(objectIDs);
		long stopTime = System.currentTimeMillis();
		
		System.out.println("Elapsed time was " + (stopTime - startTime)/1000.0 + " seconds.");
    }    
    
    
    public static void main(String[] args) 
    		throws CQLException, IOException, SQLException, CoalesceException, 
    		JSONException, CoalescePersistorException {
    	
    	//TODO change to use property file
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
