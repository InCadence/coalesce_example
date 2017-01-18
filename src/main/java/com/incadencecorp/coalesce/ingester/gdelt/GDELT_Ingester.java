package com.incadencecorp.coalesce.ingester.gdelt;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.math.NumberUtils;
import org.joda.time.DateTime;
import org.xml.sax.SAXException;

import com.incadencecorp.coalesce.common.exceptions.CoalescePersistorException;
import com.incadencecorp.coalesce.framework.CoalesceFramework;
import com.incadencecorp.coalesce.framework.CoalesceObjectFactory;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceDateTimeField;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceEntity;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceEntityTemplate;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceFieldDefinition;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecord;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecordset;
import com.incadencecorp.coalesce.framework.datamodel.ECoalesceFieldDataTypes;
import com.incadencecorp.coalesce.framework.persistance.ICoalescePersistor;
import com.incadencecorp.coalesce.framework.persistance.ServerConn;
import com.incadencecorp.coalesce.framework.persistance.accumulo.AccumuloPersistor;

public class GDELT_Ingester {
	
	/**
	 * @author Bryan Moore
	 * July 2016
	 * 
	 * GDELT_Ingester is a command line utility that will read GDELT data from a CSV file and persist
	 * that data as Coalesce Entities in a GeoMesa DataStore backed by Accumulo.
	 * 
	 * Data and documentation can be found at http://www.gdeltproject.org/data.html
	 */

	/**
	 * Method to create Entities from version 1.X GDELT data
	 * @param gdeltLine
	 * @return GDELT_Entity created from the ingest
	 */
	private GDELT_Entity createV1EntityFromLine(String gdeltLine) {
		GDELT_Entity entity = new GDELT_Entity();
		GDELT_Actor actor = new GDELT_Actor();

		CoalesceRecordset eventRecordSet = entity
				.getCoalesceRecordsetForNamePath("GDELT_DATA/Event_Section/Event_Recordset");
		CoalesceRecord eventRecord = eventRecordSet.addNew();
		String[] fields = gdeltLine.split("\t");
		if (fields.length == 58) {
			GDELT_Entity.setIntegerField(eventRecord, "GlobalEventID", fields[0]);
			GDELT_Entity.setIntegerField(eventRecord, "Day", fields[1]);
			GDELT_Entity.setIntegerField(eventRecord, "MonthYear", fields[2]);
			GDELT_Entity.setIntegerField(eventRecord, "Year", fields[3]);
			GDELT_Entity.setFloatField(eventRecord, "FractionDate", fields[4]);
			
			GDELT_Entity.setStringField(eventRecord, "Actor1Code", fields[5]);
			GDELT_Entity.setStringField(eventRecord, "Actor1Name", fields[6]);
			GDELT_Entity.setStringField(eventRecord, "Actor1CountryCode", fields[7]);
			GDELT_Entity.setStringField(eventRecord, "Actor1KnownGroupCode", fields[8]);
			GDELT_Entity.setStringField(eventRecord, "Actor1EthnicCode", fields[9]);
			GDELT_Entity.setStringField(eventRecord, "Actor1Religion1Code", fields[10]);
			GDELT_Entity.setStringField(eventRecord, "Actor1Religion2Code", fields[11]);
			GDELT_Entity.setStringField(eventRecord, "Actor1Type1Code", fields[12]);
			GDELT_Entity.setStringField(eventRecord, "Actor1Type2Code", fields[13]);
			GDELT_Entity.setStringField(eventRecord, "Actor1Type3Code", fields[14]);

			GDELT_Entity.setStringField(eventRecord, "Actor2Code", fields[15]);
			GDELT_Entity.setStringField(eventRecord, "Actor2Name", fields[16]);
			GDELT_Entity.setStringField(eventRecord, "Actor2CountryCode", fields[17]);
			GDELT_Entity.setStringField(eventRecord, "Actor2KnownGroupCode", fields[18]);
			GDELT_Entity.setStringField(eventRecord, "Actor2EthnicCode", fields[19]);
			GDELT_Entity.setStringField(eventRecord, "Actor2Religion1Code", fields[20]);
			GDELT_Entity.setStringField(eventRecord, "Actor2Religion2Code", fields[21]);
			GDELT_Entity.setStringField(eventRecord, "Actor2Type1Code", fields[22]);
			GDELT_Entity.setStringField(eventRecord, "Actor2Type2Code", fields[23]);
			GDELT_Entity.setStringField(eventRecord, "Actor2Type3Code", fields[24]);
			
			GDELT_Entity.setIntegerField(eventRecord, "IsRootEvent", fields[25]);
			GDELT_Entity.setStringField(eventRecord, "EventCode", fields[26]);
			GDELT_Entity.setStringField(eventRecord, "EventBaseCode", fields[27]);
			GDELT_Entity.setStringField(eventRecord, "EventRootCode", fields[28]);
			GDELT_Entity.setIntegerField(eventRecord, "QuadClass", fields[29]);
			GDELT_Entity.setFloatField(eventRecord, "GoldsteinScale", fields[30]);
			GDELT_Entity.setIntegerField(eventRecord, "NumMentions", fields[30]);
			GDELT_Entity.setIntegerField(eventRecord, "NumSources", fields[31]);
			GDELT_Entity.setIntegerField(eventRecord, "NumArticles", fields[32]);
			GDELT_Entity.setFloatField(eventRecord, "AvgTone", fields[34]);
			
			GDELT_Entity.setIntegerField(eventRecord, "Actor1Geo_Type", fields[35]);
			GDELT_Entity.setStringField(eventRecord, "Actor1Geo_Fullname", fields[36]);
			GDELT_Entity.setStringField(eventRecord, "Actor1Geo_CountryCode", fields[37]);
			GDELT_Entity.setStringField(eventRecord, "Actor1Geo_ADM1Code", fields[38]);
			
			//DELETE THIS
			//GDELT_Entity.setStringField(eventRecord, "Actor1Geo_ADM2Code", fields[39]);
			
			GDELT_Entity.setFloatField(eventRecord, "Actor1Geo_Lat", fields[39]);
			GDELT_Entity.setFloatField(eventRecord, "Actor1Geo_Long", fields[40]);
			GDELT_Entity.setStringField(eventRecord, "Actor1Geo_FeatureID", fields[41]);
			GDELT_Entity.setIntegerField(eventRecord, "Actor2Geo_Type", fields[42]);
			GDELT_Entity.setStringField(eventRecord, "Actor2Geo_Fullname", fields[43]);
			GDELT_Entity.setStringField(eventRecord, "Actor2Geo_CountryCode", fields[44]);
			GDELT_Entity.setStringField(eventRecord, "Actor2Geo_ADM1Code", fields[45]);
			
			//DELETE THIS
			//GDELT_Entity.setStringField(eventRecord, "Actor2Geo_ADM2Code", fields[47]);
			
			GDELT_Entity.setFloatField(eventRecord, "Actor2Geo_Lat", fields[46]);
			GDELT_Entity.setFloatField(eventRecord, "Actor2Geo_Long", fields[47]);
			GDELT_Entity.setStringField(eventRecord, "Actor2Geo_FeatureID", fields[48]);
			GDELT_Entity.setIntegerField(eventRecord, "ActionGeo_Type", fields[49]);
			GDELT_Entity.setStringField(eventRecord, "ActionGeo_Fullname", fields[50]);
			GDELT_Entity.setStringField(eventRecord, "ActionGeo_CountryCode", fields[51]);
			GDELT_Entity.setStringField(eventRecord, "ActionGeo_ADM1Code", fields[52]);
			
			//DELETE THIS
			//GDELT_Entity.setStringField(eventRecord, "ActionGeo_ADM2Code", fields[55]);
			
			GDELT_Entity.setFloatField(eventRecord, "ActionGeo_Lat", fields[53]);
			GDELT_Entity.setFloatField(eventRecord, "ActionGeo_Long", fields[54]);
			GDELT_Entity.setStringField(eventRecord, "ActionGeo_FeatureID", fields[55]);
			GDELT_Entity.setIntegerField(eventRecord, "DATEADDED", fields[56]);
			GDELT_Entity.setStringField(eventRecord, "SOURCEURL", fields[57]);

			int year = NumberUtils.toInt(fields[56].substring(0, 4));
			int month = NumberUtils.toInt(fields[56].substring(4, 6));
			int day = NumberUtils.toInt(fields[56].substring(6, 8));
			int hour = 0;
			int min = 0;
			int sec = 0;
			DateTime dt = new DateTime(year, month, day, hour, min, sec);
			((CoalesceDateTimeField) eventRecord.getFieldByName("DateTime")).setValue(dt);

			GDELT_Entity.buildAndSetGeoField(eventRecord, "Actor1Geo");
			GDELT_Entity.buildAndSetGeoField(eventRecord, "Actor2Geo");
			GDELT_Entity.buildAndSetGeoField(eventRecord, "ActionGeo");
			
			//Actor section
			CoalesceRecord actorRecord = eventRecordSet.addNew();

			GDELT_Actor.setStringField(actorRecord, "Actor1Code", fields[5]);
			GDELT_Actor.setStringField(actorRecord, "Actor1Name", fields[6]);
			GDELT_Actor.setStringField(actorRecord, "Actor1CountryCode", fields[7]);
			GDELT_Actor.setStringField(actorRecord, "Actor1KnownGroupCode", fields[8]);
			GDELT_Actor.setStringField(actorRecord, "Actor1EthnicCode", fields[9]);
			GDELT_Actor.setStringField(actorRecord, "Actor1Religion1Code", fields[10]);
			GDELT_Actor.setStringField(actorRecord, "Actor1Religion2Code", fields[11]);
			GDELT_Actor.setStringField(actorRecord, "Actor1Type1Code", fields[12]);
			GDELT_Actor.setStringField(actorRecord, "Actor1Type2Code", fields[13]);
			GDELT_Actor.setStringField(actorRecord, "Actor1Type3Code", fields[14]);

			
			GDELT_Actor.setIntegerField(actorRecord, "Actor1Geo_Type", fields[35]);
			GDELT_Actor.setStringField(actorRecord, "Actor1Geo_Fullname", fields[36]);
			GDELT_Actor.setStringField(actorRecord, "Actor1Geo_CountryCode", fields[37]);
			GDELT_Actor.setStringField(actorRecord, "Actor1Geo_ADM1Code", fields[38]);
			GDELT_Actor.setFloatField(actorRecord, "Actor1Geo_Lat", fields[39]);
			GDELT_Actor.setFloatField(actorRecord, "Actor1Geo_Long", fields[40]);
			GDELT_Actor.setStringField(actorRecord, "Actor1Geo_FeatureID", fields[41]);

			((CoalesceDateTimeField) actorRecord.getFieldByName("DateTime")).setValue(dt);

			GDELT_Actor.buildAndSetGeoField(actorRecord, "Actor1Geo");
			
			//Event section
			GDELT_Event event = new GDELT_Event();

			CoalesceRecord eventSpecificRecord = eventRecordSet.addNew();
			
		}
		return entity;
	}
	
	/**
	 * Method to create Entities from version 2.X GDELT data
	 * @param gdeltLine
	 * @return GDELT_Entity created from the ingest
	 */
	private GDELT_Entity createV2EntityFromLine(String gdeltLine) {
		GDELT_Entity entity = new GDELT_Entity();
		GDELT_Actor actor = new GDELT_Actor();

		CoalesceRecordset eventRecordSet = entity
				.getCoalesceRecordsetForNamePath("GDELT_DATA/Event_Section/Event_Recordset");
		CoalesceRecord eventRecord = eventRecordSet.addNew();
		String[] fields = gdeltLine.split("\t");
		if (fields.length == 61) {
			GDELT_Entity.setIntegerField(eventRecord, "GlobalEventID", fields[0]);
			GDELT_Entity.setIntegerField(eventRecord, "Day", fields[1]);
			GDELT_Entity.setIntegerField(eventRecord, "MonthYear", fields[2]);
			GDELT_Entity.setIntegerField(eventRecord, "Year", fields[3]);
			GDELT_Entity.setFloatField(eventRecord, "FractionDate", fields[4]);
			
			GDELT_Entity.setStringField(eventRecord, "Actor1Code", fields[5]);
			GDELT_Entity.setStringField(eventRecord, "Actor1Name", fields[6]);
			GDELT_Entity.setStringField(eventRecord, "Actor1CountryCode", fields[7]);
			GDELT_Entity.setStringField(eventRecord, "Actor1KnownGroupCode", fields[8]);
			GDELT_Entity.setStringField(eventRecord, "Actor1EthnicCode", fields[9]);
			GDELT_Entity.setStringField(eventRecord, "Actor1Religion1Code", fields[10]);
			GDELT_Entity.setStringField(eventRecord, "Actor1Religion2Code", fields[11]);
			GDELT_Entity.setStringField(eventRecord, "Actor1Type1Code", fields[12]);
			GDELT_Entity.setStringField(eventRecord, "Actor1Type2Code", fields[13]);
			GDELT_Entity.setStringField(eventRecord, "Actor1Type3Code", fields[14]);

			GDELT_Entity.setStringField(eventRecord, "Actor2Code", fields[15]);
			GDELT_Entity.setStringField(eventRecord, "Actor2Name", fields[16]);
			GDELT_Entity.setStringField(eventRecord, "Actor2CountryCode", fields[17]);
			GDELT_Entity.setStringField(eventRecord, "Actor2KnownGroupCode", fields[18]);
			GDELT_Entity.setStringField(eventRecord, "Actor2EthnicCode", fields[19]);
			GDELT_Entity.setStringField(eventRecord, "Actor2Religion1Code", fields[20]);
			GDELT_Entity.setStringField(eventRecord, "Actor2Religion2Code", fields[21]);
			GDELT_Entity.setStringField(eventRecord, "Actor2Type1Code", fields[22]);
			GDELT_Entity.setStringField(eventRecord, "Actor2Type2Code", fields[23]);
			GDELT_Entity.setStringField(eventRecord, "Actor2Type3Code", fields[24]);
			
			GDELT_Entity.setIntegerField(eventRecord, "IsRootEvent", fields[25]);
			GDELT_Entity.setStringField(eventRecord, "EventCode", fields[26]);
			GDELT_Entity.setStringField(eventRecord, "EventBaseCode", fields[27]);
			GDELT_Entity.setStringField(eventRecord, "EventRootCode", fields[28]);
			GDELT_Entity.setIntegerField(eventRecord, "QuadClass", fields[29]);
			GDELT_Entity.setFloatField(eventRecord, "GoldsteinScale", fields[30]);
			GDELT_Entity.setIntegerField(eventRecord, "NumMentions", fields[30]);
			GDELT_Entity.setIntegerField(eventRecord, "NumSources", fields[31]);
			GDELT_Entity.setIntegerField(eventRecord, "NumArticles", fields[32]);
			GDELT_Entity.setFloatField(eventRecord, "AvgTone", fields[34]);
			
			GDELT_Entity.setIntegerField(eventRecord, "Actor1Geo_Type", fields[35]);
			GDELT_Entity.setStringField(eventRecord, "Actor1Geo_Fullname", fields[36]);
			GDELT_Entity.setStringField(eventRecord, "Actor1Geo_CountryCode", fields[37]);
			GDELT_Entity.setStringField(eventRecord, "Actor1Geo_ADM1Code", fields[38]);
			GDELT_Entity.setStringField(eventRecord, "Actor1Geo_ADM2Code", fields[39]);
			
			GDELT_Entity.setFloatField(eventRecord, "Actor1Geo_Lat", fields[40]);
			GDELT_Entity.setFloatField(eventRecord, "Actor1Geo_Long", fields[41]);
			GDELT_Entity.setStringField(eventRecord, "Actor1Geo_FeatureID", fields[42]);
			GDELT_Entity.setIntegerField(eventRecord, "Actor2Geo_Type", fields[43]);
			GDELT_Entity.setStringField(eventRecord, "Actor2Geo_Fullname", fields[44]);
			GDELT_Entity.setStringField(eventRecord, "Actor2Geo_CountryCode", fields[45]);
			GDELT_Entity.setStringField(eventRecord, "Actor2Geo_ADM1Code", fields[46]);
			GDELT_Entity.setStringField(eventRecord, "Actor2Geo_ADM2Code", fields[47]);
			
			GDELT_Entity.setFloatField(eventRecord, "Actor2Geo_Lat", fields[48]);
			GDELT_Entity.setFloatField(eventRecord, "Actor2Geo_Long", fields[49]);
			GDELT_Entity.setStringField(eventRecord, "Actor2Geo_FeatureID", fields[50]);
			GDELT_Entity.setIntegerField(eventRecord, "ActionGeo_Type", fields[51]);
			GDELT_Entity.setStringField(eventRecord, "ActionGeo_Fullname", fields[52]);
			GDELT_Entity.setStringField(eventRecord, "ActionGeo_CountryCode", fields[53]);
			GDELT_Entity.setStringField(eventRecord, "ActionGeo_ADM1Code", fields[54]);
			GDELT_Entity.setStringField(eventRecord, "ActionGeo_ADM2Code", fields[55]);
			
			GDELT_Entity.setFloatField(eventRecord, "ActionGeo_Lat", fields[56]);
			GDELT_Entity.setFloatField(eventRecord, "ActionGeo_Long", fields[57]);
			GDELT_Entity.setStringField(eventRecord, "ActionGeo_FeatureID", fields[58]);
			GDELT_Entity.setIntegerField(eventRecord, "DATEADDED", fields[59]);
			GDELT_Entity.setStringField(eventRecord, "SOURCEURL", fields[60]);

			int year = NumberUtils.toInt(fields[59].substring(0, 4));
			int month = NumberUtils.toInt(fields[59].substring(4, 6));
			int day = NumberUtils.toInt(fields[59].substring(6, 8));
            int hour = NumberUtils.toInt(fields[59].substring(8, 10));
            int min = NumberUtils.toInt(fields[59].substring(10, 12));
            int sec = NumberUtils.toInt(fields[59].substring(12, 14));
			DateTime dt = new DateTime(year, month, day, hour, min, sec);
			((CoalesceDateTimeField) eventRecord.getFieldByName("DateTime")).setValue(dt);

			GDELT_Entity.buildAndSetGeoField(eventRecord, "Actor1Geo");
			GDELT_Entity.buildAndSetGeoField(eventRecord, "Actor2Geo");
			GDELT_Entity.buildAndSetGeoField(eventRecord, "ActionGeo");
			
			//Actor section
			CoalesceRecord actorRecord = eventRecordSet.addNew();

			GDELT_Actor.setStringField(actorRecord, "Actor1Code", fields[5]);
			GDELT_Actor.setStringField(actorRecord, "Actor1Name", fields[6]);
			GDELT_Actor.setStringField(actorRecord, "Actor1CountryCode", fields[7]);
			GDELT_Actor.setStringField(actorRecord, "Actor1KnownGroupCode", fields[8]);
			GDELT_Actor.setStringField(actorRecord, "Actor1EthnicCode", fields[9]);
			GDELT_Actor.setStringField(actorRecord, "Actor1Religion1Code", fields[10]);
			GDELT_Actor.setStringField(actorRecord, "Actor1Religion2Code", fields[11]);
			GDELT_Actor.setStringField(actorRecord, "Actor1Type1Code", fields[12]);
			GDELT_Actor.setStringField(actorRecord, "Actor1Type2Code", fields[13]);
			GDELT_Actor.setStringField(actorRecord, "Actor1Type3Code", fields[14]);

			
			GDELT_Actor.setIntegerField(actorRecord, "Actor1Geo_Type", fields[35]);
			GDELT_Actor.setStringField(actorRecord, "Actor1Geo_Fullname", fields[36]);
			GDELT_Actor.setStringField(actorRecord, "Actor1Geo_CountryCode", fields[37]);
			GDELT_Actor.setStringField(actorRecord, "Actor1Geo_ADM1Code", fields[38]);
			GDELT_Actor.setStringField(actorRecord, "Actor1Geo_ADM2Code", fields[39]);
			GDELT_Actor.setFloatField(actorRecord, "Actor1Geo_Lat", fields[40]);
			GDELT_Actor.setFloatField(actorRecord, "Actor1Geo_Long", fields[41]);
			GDELT_Actor.setStringField(actorRecord, "Actor1Geo_FeatureID", fields[42]);

			((CoalesceDateTimeField) actorRecord.getFieldByName("DateTime")).setValue(dt);

			GDELT_Actor.buildAndSetGeoField(actorRecord, "Actor1Geo");
			
			//Event section
			GDELT_Event event = new GDELT_Event();

			CoalesceRecord eventSpecificRecord = eventRecordSet.addNew();
			
		}
		return entity;
	}
	
	private GDELT_Actor createActorFromLine(String gdeltLine) {
		GDELT_Actor actor = new GDELT_Actor();

		CoalesceRecordset eventRecordSet = actor
				.getCoalesceRecordsetForNamePath("GDELT_DATA/Actor_Section/Actor_Recordset");
		CoalesceRecord actorRecord = eventRecordSet.addNew();
		String[] fields = gdeltLine.split("\t");

		GDELT_Entity.setStringField(actorRecord, "Actor1Code", fields[5]);
		GDELT_Entity.setStringField(actorRecord, "Actor1Name", fields[6]);
		GDELT_Entity.setStringField(actorRecord, "Actor1CountryCode", fields[7]);
		GDELT_Entity.setStringField(actorRecord, "Actor1KnownGroupCode", fields[8]);
		GDELT_Entity.setStringField(actorRecord, "Actor1EthnicCode", fields[9]);
		GDELT_Entity.setStringField(actorRecord, "Actor1Religion1Code", fields[10]);
		GDELT_Entity.setStringField(actorRecord, "Actor1Religion2Code", fields[11]);
		GDELT_Entity.setStringField(actorRecord, "Actor1Type1Code", fields[12]);
		GDELT_Entity.setStringField(actorRecord, "Actor1Type2Code", fields[13]);
		GDELT_Entity.setStringField(actorRecord, "Actor1Type3Code", fields[14]);

		
		GDELT_Entity.setIntegerField(actorRecord, "Actor1Geo_Type", fields[35]);
		GDELT_Entity.setStringField(actorRecord, "Actor1Geo_Fullname", fields[36]);
		GDELT_Entity.setStringField(actorRecord, "Actor1Geo_CountryCode", fields[37]);
		GDELT_Entity.setStringField(actorRecord, "Actor1Geo_ADM1Code", fields[38]);
		GDELT_Entity.setFloatField(actorRecord, "Actor1Geo_Lat", fields[39]);
		GDELT_Entity.setFloatField(actorRecord, "Actor1Geo_Long", fields[40]);
		GDELT_Entity.setStringField(actorRecord, "Actor1Geo_FeatureID", fields[41]);

		int year = NumberUtils.toInt(fields[56].substring(0, 4));
		int month = NumberUtils.toInt(fields[56].substring(4, 6));
		int day = NumberUtils.toInt(fields[56].substring(6, 8));
		int hour = 0;
		int min = 0;
		int sec = 0;
		DateTime dt = new DateTime(year, month, day, hour, min, sec);
		((CoalesceDateTimeField) actorRecord.getFieldByName("DateTime")).setValue(dt);

		GDELT_Entity.buildAndSetGeoField(actorRecord, "Actor1Geo");
			
		return actor;
	}

	public List<CoalesceEntity> loadRecordsFromFile(File file) throws IOException {
		List<CoalesceEntity> entities = new ArrayList<>();
		try (BufferedReader fr = new BufferedReader(new FileReader(file))) {
			String line;
			while ((line = fr.readLine()) != null) {
				entities.add(createV2EntityFromLine(line));
			}
		}
		return entities;
	}

	public int persistRecordsFromFile(ICoalescePersistor persistor, File file)
			throws IOException, SAXException, CoalescePersistorException {
		CoalesceFramework coalesceFramework = new CoalesceFramework();
		coalesceFramework.setAuthoritativePersistor(persistor);
		int count = 0;
		boolean firstEntity = true;
		List<CoalesceEntity> entities = new ArrayList<>();
		try (BufferedReader fr = new BufferedReader(new FileReader(file))) {
			String line;
			long beginTime = System.currentTimeMillis();
			while ((line = fr.readLine()) != null) {
				try {
					GDELT_Entity entity = createV2EntityFromLine(line);
					if (firstEntity) {
						CoalesceFramework framework = new CoalesceFramework();
						framework.setAuthoritativePersistor(persistor);
						framework.saveCoalesceEntityTemplate(CoalesceEntityTemplate.create(entity));
						framework.close();
						CoalesceObjectFactory.register(GDELT_Entity.class);
						firstEntity = false;
					}
					entities.add(entity);
					count++;
					if (count % 100 == 0) {
						long elapsedTime = System.currentTimeMillis() - beginTime;
						double rate = (double)count / (double)elapsedTime * 1000d;
						System.out.println(String.format("%d  @ rate of %f per second.   Current ms: %d", count,rate,System.currentTimeMillis()));
						persistor.saveEntity(true, entities.toArray(new CoalesceEntity[entities.size()]));
						entities.clear();
					}
				} catch (CoalescePersistorException e) {
					e.printStackTrace();
				}
			}
			if (!entities.isEmpty()) {
				persistor.saveEntity(true, entities.toArray(new CoalesceEntity[entities.size()]));
				count += entities.size();
			}
		}
		coalesceFramework.close();
		return count;
	}

	public static void main(String[] args)
			throws FileNotFoundException, IOException, CoalescePersistorException, SAXException {
		if (args.length != 2) {
			System.out.println("Usage: java -cp $CLASSPATH com.incadencecorp.coalesce.ingester.gdelt.GDELT_Ingester configfile datafile");
		} else {
			GDELT_Ingester ingester = new GDELT_Ingester();
			Properties props = new Properties();
			props.load(new FileReader(new File(args[0])));
			File inputFile = new File(args[1]);
			
			//BDP Undev cluster
			/*
			String dbName = "bdp";
			String zookeepers = "10.10.10.74";
			String user = "root";
			String password = "accumulo";
			*/
			
			//Christina's machine
			String dbName = "accumulo";
			String zookeepers = "10.59.62.14";
			String user = "root";
			String password = "secret";
			
			ServerConn conn = new ServerConn.Builder().db(dbName).serverName(zookeepers).user(user).password(password).build();
			AccumuloPersistor persistor = new AccumuloPersistor(conn);
			int count = ingester.persistRecordsFromFile(persistor, inputFile);
			System.out.println("Persisted " + count + " records");
			persistor.close();
		}
	}
}
