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
import com.incadencecorp.coalesce.framework.datamodel.CoalesceCoordinateField;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceDateTimeField;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceEntity;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceEntityTemplate;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceFieldDefinition;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceFloatField;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceIntegerField;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecord;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecordset;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceSection;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceStringField;
import com.incadencecorp.coalesce.framework.datamodel.ECoalesceFieldDataTypes;
import com.incadencecorp.coalesce.framework.persistance.ICoalescePersistor;
import com.incadencecorp.coalesce.framework.persistance.ServerConn;
import com.incadencecorp.coalesce.framework.persistance.accumulo.AccumuloPersistor;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;

public class GDELT_Ingester {

	private static String NAME = "GDELT_DATA";
	private static String SOURCE = "gdeltproject.org";
	private static String VERSION = "1.0";
	private static String TITLE = "G2_Core_GDELT";
	private static String SECTION = "Event_Section";
	private static String RECORDSET = "Event_Recordset";

	public static String getRecordSetName() {
		return NAME + "/" + SECTION + "/" + RECORDSET;
	}

	public static String getQueryName() {
		return NAME + "_" + SOURCE + "_" + VERSION + "." + SECTION + "." + RECORDSET;
	}

	private CoalesceEntity createNewEntity() {
		CoalesceEntity entity = CoalesceEntity.create(NAME, SOURCE, VERSION, "", "", TITLE);
		CoalesceSection eventSection = CoalesceSection.create(entity, "Event_Section");
		CoalesceRecordset eventRecordSet = CoalesceRecordset.create(eventSection, "Event_Recordset");
		CoalesceFieldDefinition.create(eventRecordSet, "GlobalEventID", ECoalesceFieldDataTypes.INTEGER_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Day", ECoalesceFieldDataTypes.INTEGER_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "MonthYear", ECoalesceFieldDataTypes.INTEGER_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Year", ECoalesceFieldDataTypes.INTEGER_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "FractionDate", ECoalesceFieldDataTypes.FLOAT_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Actor1Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Actor1Name", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Actor1CountryCode", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Actor1KnownGroupCode", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Actor1EthnicCode", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Actor1Religion1Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Actor1Religion2Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Actor1Type1Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Actor1Type2Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Actor1Type3Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Actor2Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Actor2Name", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Actor2CountryCode", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Actor2KnownGroupCode", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Actor2EthnicCode", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Actor2Religion1Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Actor2Religion2Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Actor2Type1Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Actor2Type2Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Actor2Type3Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "IsRootEvent", ECoalesceFieldDataTypes.INTEGER_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "EventCode", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "EventBaseCode", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "EventRootCode", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "QuadClass", ECoalesceFieldDataTypes.INTEGER_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "GoldsteinScale", ECoalesceFieldDataTypes.FLOAT_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "NumMentions", ECoalesceFieldDataTypes.INTEGER_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "NumSources", ECoalesceFieldDataTypes.INTEGER_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "NumArticles", ECoalesceFieldDataTypes.INTEGER_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "AvgTone", ECoalesceFieldDataTypes.FLOAT_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Actor1Geo_Type", ECoalesceFieldDataTypes.INTEGER_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Actor1Geo_Fullname", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Actor1Geo_CountryCode", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Actor1Geo_ADM1Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Actor1Geo_ADM2Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Actor1Geo_Lat", ECoalesceFieldDataTypes.FLOAT_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Actor1Geo_Long", ECoalesceFieldDataTypes.FLOAT_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Actor1Geo_FeatureID", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Actor2Geo_Type", ECoalesceFieldDataTypes.INTEGER_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Actor2Geo_Fullname", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Actor2Geo_CountryCode", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Actor2Geo_ADM1Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Actor2Geo_ADM2Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Actor2Geo_Lat", ECoalesceFieldDataTypes.FLOAT_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Actor2Geo_Long", ECoalesceFieldDataTypes.FLOAT_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Actor2Geo_FeatureID", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "ActionGeo_Type", ECoalesceFieldDataTypes.INTEGER_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "ActionGeo_Fullname", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "ActionGeo_CountryCode", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "ActionGeo_ADM1Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "ActionGeo_ADM2Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "ActionGeo_Lat", ECoalesceFieldDataTypes.FLOAT_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "ActionGeo_Long", ECoalesceFieldDataTypes.FLOAT_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "ActionGeo_FeatureID", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "DATEADDED", ECoalesceFieldDataTypes.INTEGER_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "SOURCEURL", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "DateTime", ECoalesceFieldDataTypes.DATE_TIME_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Actor1Geo_Location",
				ECoalesceFieldDataTypes.GEOCOORDINATE_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Actor2Geo_Location",
				ECoalesceFieldDataTypes.GEOCOORDINATE_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "ActionGeo_Location",
				ECoalesceFieldDataTypes.GEOCOORDINATE_TYPE);

		return entity;
	}

	private void setIntegerField(CoalesceRecord eventRecord, String name, String value) {
		((CoalesceIntegerField) eventRecord.getFieldByName(name)).setValue(NumberUtils.toInt(value));
	}

	private void setStringField(CoalesceRecord eventRecord, String name, String value) {
		((CoalesceStringField) eventRecord.getFieldByName(name)).setValue(value);
	}

	private void setFloatField(CoalesceRecord eventRecord, String name, String value) {
		((CoalesceFloatField) eventRecord.getFieldByName(name)).setValue(NumberUtils.toFloat(value));
	}

	private void buildAndSetGeoField(CoalesceRecord eventRecord, String prefix) {
		GeometryFactory factory = new GeometryFactory();
		try {
			Coordinate coord = new Coordinate(
					((CoalesceFloatField) eventRecord.getFieldByName(prefix + "_Long")).getValue(),
					((CoalesceFloatField) eventRecord.getFieldByName(prefix + "_Lat")).getValue());
			((CoalesceCoordinateField) eventRecord.getFieldByName(prefix + "_Location"))
					.setValue(factory.createPoint(coord));
		} catch (CoalesceDataFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private CoalesceEntity createEntityFromLine(String gdeltLine) {
		CoalesceEntity entity = createNewEntity();

		CoalesceRecordset eventRecordSet = entity
				.getCoalesceRecordsetForNamePath("GDELT_DATA/Event_Section/Event_Recordset");
		CoalesceRecord eventRecord = eventRecordSet.addNew();
		String[] fields = gdeltLine.split("\t");
		if (fields.length == 61) {
			setIntegerField(eventRecord, "GlobalEventID", fields[0]);
			setIntegerField(eventRecord, "Day", fields[1]);
			setIntegerField(eventRecord, "MonthYear", fields[2]);
			setIntegerField(eventRecord, "Year", fields[3]);
			setFloatField(eventRecord, "FractionDate", fields[4]);
			setStringField(eventRecord, "Actor1Code", fields[5]);
			setStringField(eventRecord, "Actor1Name", fields[6]);
			setStringField(eventRecord, "Actor1CountryCode", fields[7]);
			setStringField(eventRecord, "Actor1KnownGroupCode", fields[8]);
			setStringField(eventRecord, "Actor1EthnicCode", fields[9]);
			setStringField(eventRecord, "Actor1Religion1Code", fields[10]);
			setStringField(eventRecord, "Actor1Religion2Code", fields[11]);
			setStringField(eventRecord, "Actor1Type1Code", fields[12]);
			setStringField(eventRecord, "Actor1Type2Code", fields[13]);
			setStringField(eventRecord, "Actor1Type3Code", fields[14]);

			setStringField(eventRecord, "Actor2Code", fields[15]);
			setStringField(eventRecord, "Actor2Name", fields[16]);
			setStringField(eventRecord, "Actor2CountryCode", fields[17]);
			setStringField(eventRecord, "Actor2KnownGroupCode", fields[18]);
			setStringField(eventRecord, "Actor2EthnicCode", fields[19]);
			setStringField(eventRecord, "Actor2Religion1Code", fields[20]);
			setStringField(eventRecord, "Actor2Religion2Code", fields[21]);
			setStringField(eventRecord, "Actor2Type1Code", fields[22]);
			setStringField(eventRecord, "Actor2Type2Code", fields[23]);
			setStringField(eventRecord, "Actor2Type3Code", fields[24]);
			setIntegerField(eventRecord, "IsRootEvent", fields[25]);
			setStringField(eventRecord, "EventCode", fields[26]);
			setStringField(eventRecord, "EventBaseCode", fields[27]);
			setStringField(eventRecord, "EventRootCode", fields[28]);
			setIntegerField(eventRecord, "QuadClass", fields[29]);
			setFloatField(eventRecord, "GoldsteinScale", fields[30]);
			setIntegerField(eventRecord, "NumMentions", fields[30]);
			setIntegerField(eventRecord, "NumSources", fields[31]);
			setIntegerField(eventRecord, "NumArticles", fields[32]);
			setFloatField(eventRecord, "AvgTone", fields[34]);
			setIntegerField(eventRecord, "Actor1Geo_Type", fields[35]);
			setStringField(eventRecord, "Actor1Geo_Fullname", fields[36]);
			setStringField(eventRecord, "Actor1Geo_CountryCode", fields[37]);
			setStringField(eventRecord, "Actor1Geo_ADM1Code", fields[38]);
			setStringField(eventRecord, "Actor1Geo_ADM2Code", fields[39]);
			setFloatField(eventRecord, "Actor1Geo_Lat", fields[40]);
			setFloatField(eventRecord, "Actor1Geo_Long", fields[41]);
			setStringField(eventRecord, "Actor1Geo_FeatureID", fields[42]);
			setIntegerField(eventRecord, "Actor2Geo_Type", fields[43]);
			setStringField(eventRecord, "Actor2Geo_Fullname", fields[44]);
			setStringField(eventRecord, "Actor2Geo_CountryCode", fields[45]);
			setStringField(eventRecord, "Actor2Geo_ADM1Code", fields[46]);
			setStringField(eventRecord, "Actor2Geo_ADM2Code", fields[47]);
			setFloatField(eventRecord, "Actor2Geo_Lat", fields[48]);
			setFloatField(eventRecord, "Actor2Geo_Long", fields[49]);
			setStringField(eventRecord, "Actor2Geo_FeatureID", fields[50]);
			setIntegerField(eventRecord, "ActionGeo_Type", fields[51]);
			setStringField(eventRecord, "ActionGeo_Fullname", fields[52]);
			setStringField(eventRecord, "ActionGeo_CountryCode", fields[53]);
			setStringField(eventRecord, "ActionGeo_ADM1Code", fields[54]);
			setStringField(eventRecord, "ActionGeo_ADM2Code", fields[55]);
			setFloatField(eventRecord, "ActionGeo_Lat", fields[56]);
			setFloatField(eventRecord, "ActionGeo_Long", fields[57]);
			setStringField(eventRecord, "ActionGeo_FeatureID", fields[58]);
			setIntegerField(eventRecord, "DATEADDED", fields[59]);
			setStringField(eventRecord, "SOURCEURL", fields[60]);

			int year = NumberUtils.toInt(fields[59].substring(0, 4));
			int month = NumberUtils.toInt(fields[59].substring(4, 6));
			int day = NumberUtils.toInt(fields[59].substring(6, 8));
			int hour = NumberUtils.toInt(fields[59].substring(8, 10));
			int min = NumberUtils.toInt(fields[59].substring(10, 12));
			int sec = NumberUtils.toInt(fields[59].substring(12, 14));
			DateTime dt = new DateTime(year, month, day, hour, min, sec);
			((CoalesceDateTimeField) eventRecord.getFieldByName("DateTime")).setValue(dt);

			buildAndSetGeoField(eventRecord, "Actor1Geo");
			buildAndSetGeoField(eventRecord, "Actor2Geo");
			buildAndSetGeoField(eventRecord, "ActionGeo");

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
					if (count % 2 == 0) {
						System.out.println();
						System.out.print(count);
						persistor.saveEntity(true, entities.toArray(new CoalesceEntity[entities.size()]));
						entities.clear();
					}
					System.out.print(".");
				} catch (CoalescePersistorException e) {
					// TODO Auto-generated catch block
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
			System.out.println(
					"Usage:  java -cp $CLASSPATH com.incadencecorp.coalesce.ingester.gdelt.GDELT_Ingester configfile datafile");
		} else {
			System.out.print("Press enter to continue");
			System.in.read();
			System.out.println();
			GDELT_Ingester ingester = new GDELT_Ingester();
			Properties props = new Properties();
			props.load(new FileReader(new File(args[0])));
			File inputFile = new File(args[1]);
			String dbName = props.getProperty("database");
			String zookeepers = props.getProperty("zookeepers");
			String user = props.getProperty("userid");
			String password = props.getProperty("password");
			ServerConn conn = new ServerConn.Builder().db(dbName).serverName(zookeepers).user(user).password(password)
					.build();
			AccumuloPersistor persistor = new AccumuloPersistor(conn);
			int count = ingester.persistRecordsFromFile(persistor, inputFile);
			System.out.println("Persisted " + count + " records");
			persistor.close();
		}
	}
}
