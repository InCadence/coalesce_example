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

import com.incadencecorp.coalesce.common.exceptions.CoalesceDataFormatException;
import com.incadencecorp.coalesce.common.exceptions.CoalescePersistorException;
import com.incadencecorp.coalesce.framework.CoalesceFramework;
import com.incadencecorp.coalesce.framework.CoalesceObjectFactory;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceCoordinateField;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceDateTimeField;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceEntity;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceEntityTemplate;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceFloatField;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecord;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecordset;
import com.incadencecorp.coalesce.framework.persistance.ICoalescePersistor;
import com.incadencecorp.coalesce.framework.persistance.ServerConn;
import com.incadencecorp.coalesce.framework.persistance.accumulo.AccumuloPersistor;
import com.vividsolutions.jts.geom.Coordinate;

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

	private CoalesceEntity createEntityFromLine(String gdeltLine) {
		CoalesceEntity entity = new GDELT_Entity();

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
		}
		return entity;
	}

	public List<CoalesceEntity> loadRecordsFromFile(File file) throws IOException {
		List<CoalesceEntity> entities = new ArrayList<>();
		try (BufferedReader fr = new BufferedReader(new FileReader(file))) {
			String line;
			while ((line = fr.readLine()) != null) {
				entities.add(createEntityFromLine(line));
			}
		}
		return entities;
	}

	public int persistRecordsFromFile(ICoalescePersistor persistor, File file)
			throws IOException, SAXException, CoalescePersistorException {
		CoalesceFramework coalesceFramework = new CoalesceFramework();
		coalesceFramework.initialize(persistor);
		boolean firstEntity = true;
		int count = 0;
		List<CoalesceEntity> entities = new ArrayList<>();
		try (BufferedReader fr = new BufferedReader(new FileReader(file))) {
			String line;
			while ((line = fr.readLine()) != null) {
				try {
					CoalesceEntity entity = createEntityFromLine(line);
					
					if (firstEntity) {
						coalesceFramework.saveCoalesceEntityTemplate(CoalesceEntityTemplate.create(entity));
						firstEntity = false;
					}
					
					entities.add(entity);
					count++;
					if (count % 100 == 0) {
						System.out.println();
						System.out.print(count);
						persistor.saveEntity(true, entities.toArray(new CoalesceEntity[entities.size()]));
						entities.clear();
					}
					System.out.print(".");
				} catch (CoalescePersistorException e) {
					e.printStackTrace();
				}
			}
			if (!entities.isEmpty()) {
				persistor.saveEntity(true, entities.toArray(new CoalesceEntity[entities.size()]));
				count += entities.size();
			}
			System.out.println();
		}
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
			String dbName = props.getProperty("database");
			String zookeepers = props.getProperty("zookeepers");
			String user = props.getProperty("userid");
			String password = props.getProperty("password");
			ServerConn conn = new ServerConn.Builder().db(dbName).serverName(zookeepers).user(user).password(password).build();
			AccumuloPersistor persistor = new AccumuloPersistor(conn);
			CoalesceFramework framework = new CoalesceFramework();
			framework.initialize(persistor);
			framework.saveCoalesceEntityTemplate(CoalesceEntityTemplate.create(new GDELT_Entity()));
			CoalesceObjectFactory.register(GDELT_Entity.class);
			int count = ingester.persistRecordsFromFile(persistor, inputFile);
			System.out.println("Persisted " + count + " records");
			persistor.close();
		}
	}
}
