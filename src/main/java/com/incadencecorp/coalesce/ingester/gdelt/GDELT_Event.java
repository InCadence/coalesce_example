package com.incadencecorp.coalesce.ingester.gdelt;

import com.incadencecorp.coalesce.framework.datamodel.CoalesceFieldDefinition;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecord;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecordset;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceSection;
import com.incadencecorp.coalesce.framework.datamodel.ECoalesceFieldDataTypes;

public class GDELT_Event extends GDELT_Entity {
	
	private static String NAME = "GDELT_EVENT";
	private static String SOURCE = "gdeltproject.org";
	private static String VERSION = "1.0";
	private static String TITLE = "G2_Core_GDELT_EVENT";
	
	public GDELT_Event() {
		initialize();
		setName(NAME);
		setSource(SOURCE);
		setVersion(VERSION);
		setTitle(TITLE);
		CoalesceSection eventSection = CoalesceSection.create(this, "Event_Section");
		CoalesceRecordset eventRecordSet = CoalesceRecordset.create(eventSection, "Event_Recordset");
		CoalesceFieldDefinition.create(eventRecordSet, "GlobalEventID", ECoalesceFieldDataTypes.INTEGER_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Day", ECoalesceFieldDataTypes.INTEGER_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "MonthYear", ECoalesceFieldDataTypes.INTEGER_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Year", ECoalesceFieldDataTypes.INTEGER_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "FractionDate", ECoalesceFieldDataTypes.FLOAT_TYPE);
		
		CoalesceFieldDefinition.create(eventRecordSet, "Actor1Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Actor2Code", ECoalesceFieldDataTypes.STRING_TYPE);
		
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
		
		//Keep to remember where each actor was during the event?
		CoalesceFieldDefinition.create(eventRecordSet, "Actor1Geo_Location",
				ECoalesceFieldDataTypes.GEOCOORDINATE_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Actor2Geo_Location",
				ECoalesceFieldDataTypes.GEOCOORDINATE_TYPE);
		
		CoalesceFieldDefinition.create(eventRecordSet, "ActionGeo_Location",
				ECoalesceFieldDataTypes.GEOCOORDINATE_TYPE);

		CoalesceRecord eventRecord = eventRecordSet.addNew();
	}
}
