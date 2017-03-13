package com.incadencecorp.coalesce.ingester.gdelt;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import com.incadencecorp.coalesce.common.exceptions.CoalesceDataFormatException;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceBooleanField;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceCoordinateField;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceEntity;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceFieldDefinition;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceFloatField;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceIntegerField;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecord;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecordset;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceSection;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceStringField;
import com.incadencecorp.coalesce.framework.datamodel.ECoalesceFieldDataTypes;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

public class OEEntity extends CoalesceEntity {

	private static String NAME = "OEEntity";
	private static String TITLE = "G2CoreEntity";
	protected static String SOURCE = "G2Core";
	protected static String VERSION = "0.1";
	private static String SECTION = "EntitySection";
	private static String RECORDSET = "EntityRecordset";

	public static String getRecordSetName() {
		return NAME + "/" + SECTION + "/" + RECORDSET;
	}

	public static String getQueryName() {
		return NAME + SOURCE + VERSION + "." + SECTION + "." + RECORDSET;
	}

	public OEEntity() {
		initialize();
		setName(NAME);
		setSource(SOURCE);
		setVersion(VERSION);
		setTitle(TITLE);
		
		CoalesceSection oeSection = CoalesceSection.create(this, "OESection");
		CoalesceRecordset oeRecordset = OERecord.createRecordSet(oeSection, "OERecordset");
		
		oeRecordset.addNew();
		
	}

	public static void setIntegerField(CoalesceRecord eventRecord, String name, String value) {
		if (StringUtils.isNotBlank(value) && NumberUtils.isNumber(value)) {
			((CoalesceIntegerField) eventRecord.getFieldByName(name)).setValue(NumberUtils.toInt(value));
		}
	}

	public static void setStringField(CoalesceRecord eventRecord, String name, String value) {
		if (StringUtils.isNotBlank(value)) {
			((CoalesceStringField) eventRecord.getFieldByName(name)).setValue(value);
		}
	}

	public static void setFloatField(CoalesceRecord eventRecord, String name, String value) {
		if (StringUtils.isNotBlank(value)) {
			((CoalesceFloatField) eventRecord.getFieldByName(name)).setValue(NumberUtils.toFloat(value));
		}
	}

	public static void setBooleanField(CoalesceRecord eventRecord, String name, Boolean value) {
			((CoalesceBooleanField) eventRecord.getFieldByName(name)).setValue(value);
	}

	public static void buildAndSetGeoField(CoalesceRecord eventRecord, String prefix, Float lat, Float lon) {
		GeometryFactory factory = new GeometryFactory();
		try {
			if ((lat != null) && (lon != null) ) {
				Point point = factory.createPoint(new Coordinate(lon, lat));
				((CoalesceCoordinateField) eventRecord.getFieldByName(prefix + "Location")).setValue(point);
			}
		} catch (CoalesceDataFormatException e) {
			e.printStackTrace();
		}
	}
}
