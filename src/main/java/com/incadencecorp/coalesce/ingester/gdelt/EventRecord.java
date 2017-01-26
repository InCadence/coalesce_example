package com.incadencecorp.coalesce.ingester.gdelt;

import com.incadencecorp.coalesce.framework.datamodel.CoalesceFieldDefinition;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecord;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecordset;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceSection;
import com.incadencecorp.coalesce.framework.datamodel.ECoalesceFieldDataTypes;

public class EventRecord extends OERecord {

	public EventRecord() {

	}

	public EventRecord(CoalesceRecord record) {
		super(record);

	}
	
	public static CoalesceRecordset createRecordSet(CoalesceSection section, String pathName) {
		CoalesceRecordset eventRecordSet = CoalesceRecordset.create(section, pathName);
		
		CoalesceFieldDefinition.create(eventRecordSet, "GlobalEventID", ECoalesceFieldDataTypes.INTEGER_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Day", ECoalesceFieldDataTypes.INTEGER_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "MonthYear", ECoalesceFieldDataTypes.INTEGER_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "Year", ECoalesceFieldDataTypes.INTEGER_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "FractionDate", ECoalesceFieldDataTypes.FLOAT_TYPE);
		
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

		CoalesceFieldDefinition.create(eventRecordSet, "ActionGeoType", ECoalesceFieldDataTypes.INTEGER_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "ActionGeoFullname", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "ActionGeoCountryCode", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "ActionGeoADM1Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "ActionGeoADM2Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "ActionGeoFeatureID", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "DateAdded", ECoalesceFieldDataTypes.INTEGER_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "SourceURL", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(eventRecordSet, "DateTime", ECoalesceFieldDataTypes.DATE_TIME_TYPE);
		
		CoalesceFieldDefinition.create(eventRecordSet, "ActionGeoLocation",
				ECoalesceFieldDataTypes.GEOCOORDINATE_TYPE);

		return eventRecordSet;
	}

}
