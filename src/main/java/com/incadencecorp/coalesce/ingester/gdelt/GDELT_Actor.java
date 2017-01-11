package com.incadencecorp.coalesce.ingester.gdelt;

import com.incadencecorp.coalesce.framework.datamodel.CoalesceFieldDefinition;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecord;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecordset;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceSection;
import com.incadencecorp.coalesce.framework.datamodel.ECoalesceFieldDataTypes;

public class GDELT_Actor extends GDELT_Entity {

	private static String NAME = "GDELT_ACTOR";
	private static String SOURCE = "gdeltproject.org";
	private static String VERSION = "1.0";
	private static String TITLE = "G2_Core_GDELT_ACTOR";
	
	public GDELT_Actor() {
		initialize();
		setName(NAME);
		setSource(SOURCE);
		setVersion(VERSION);
		setTitle(TITLE);
		
		CoalesceSection actorSection = CoalesceSection.create(this, "Actor_Section");
		CoalesceRecordset actorRecordSet = CoalesceRecordset.create(actorSection, "Actor_Recordset");
		//CoalesceFieldDefinition.create(actorRecordSet, "GlobalEventID", ECoalesceFieldDataTypes.INTEGER_TYPE);
		//CoalesceFieldDefinition.create(actorRecordSet, "Day", ECoalesceFieldDataTypes.INTEGER_TYPE);
		//CoalesceFieldDefinition.create(actorRecordSet, "MonthYear", ECoalesceFieldDataTypes.INTEGER_TYPE);
		//CoalesceFieldDefinition.create(actorRecordSet, "Year", ECoalesceFieldDataTypes.INTEGER_TYPE);
		//CoalesceFieldDefinition.create(actorRecordSet, "FractionDate", ECoalesceFieldDataTypes.FLOAT_TYPE);
		CoalesceFieldDefinition.create(actorRecordSet, "Actor1Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(actorRecordSet, "Actor1Name", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(actorRecordSet, "Actor1CountryCode", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(actorRecordSet, "Actor1KnownGroupCode", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(actorRecordSet, "Actor1EthnicCode", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(actorRecordSet, "Actor1Religion1Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(actorRecordSet, "Actor1Religion2Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(actorRecordSet, "Actor1Type1Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(actorRecordSet, "Actor1Type2Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(actorRecordSet, "Actor1Type3Code", ECoalesceFieldDataTypes.STRING_TYPE);
		
		/*
		CoalesceFieldDefinition.create(actorRecrod, "Actor2Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(actorRecrod, "Actor2Name", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(actorRecrod, "Actor2CountryCode", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(actorRecrod, "Actor2KnownGroupCode", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(actorRecrod, "Actor2EthnicCode", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(actorRecrod, "Actor2Religion1Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(actorRecrod, "Actor2Religion2Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(actorRecrod, "Actor2Type1Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(actorRecrod, "Actor2Type2Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(actorRecrod, "Actor2Type3Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(actorRecrod, "IsRootEvent", ECoalesceFieldDataTypes.INTEGER_TYPE);
		CoalesceFieldDefinition.create(actorRecrod, "EventCode", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(actorRecrod, "EventBaseCode", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(actorRecrod, "EventRootCode", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(actorRecrod, "QuadClass", ECoalesceFieldDataTypes.INTEGER_TYPE);
		CoalesceFieldDefinition.create(actorRecrod, "GoldsteinScale", ECoalesceFieldDataTypes.FLOAT_TYPE);
		CoalesceFieldDefinition.create(actorRecrod, "NumMentions", ECoalesceFieldDataTypes.INTEGER_TYPE);
		CoalesceFieldDefinition.create(actorRecrod, "NumSources", ECoalesceFieldDataTypes.INTEGER_TYPE);
		CoalesceFieldDefinition.create(actorRecrod, "NumArticles", ECoalesceFieldDataTypes.INTEGER_TYPE);
		CoalesceFieldDefinition.create(actorRecrod, "AvgTone", ECoalesceFieldDataTypes.FLOAT_TYPE);
		*/
		
		CoalesceFieldDefinition.create(actorRecordSet, "Actor1Geo_Type", ECoalesceFieldDataTypes.INTEGER_TYPE);
		CoalesceFieldDefinition.create(actorRecordSet, "Actor1Geo_Fullname", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(actorRecordSet, "Actor1Geo_CountryCode", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(actorRecordSet, "Actor1Geo_ADM1Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(actorRecordSet, "Actor1Geo_ADM2Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(actorRecordSet, "Actor1Geo_Lat", ECoalesceFieldDataTypes.FLOAT_TYPE);
		CoalesceFieldDefinition.create(actorRecordSet, "Actor1Geo_Long", ECoalesceFieldDataTypes.FLOAT_TYPE);
		CoalesceFieldDefinition.create(actorRecordSet, "Actor1Geo_FeatureID", ECoalesceFieldDataTypes.STRING_TYPE);
		/*
		CoalesceFieldDefinition.create(actorRecrod, "Actor2Geo_Type", ECoalesceFieldDataTypes.INTEGER_TYPE);
		CoalesceFieldDefinition.create(actorRecrod, "Actor2Geo_Fullname", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(actorRecrod, "Actor2Geo_CountryCode", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(actorRecrod, "Actor2Geo_ADM1Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(actorRecrod, "Actor2Geo_ADM2Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(actorRecrod, "Actor2Geo_Lat", ECoalesceFieldDataTypes.FLOAT_TYPE);
		CoalesceFieldDefinition.create(actorRecrod, "Actor2Geo_Long", ECoalesceFieldDataTypes.FLOAT_TYPE);
		CoalesceFieldDefinition.create(actorRecrod, "Actor2Geo_FeatureID", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(actorRecrod, "ActionGeo_Type", ECoalesceFieldDataTypes.INTEGER_TYPE);
		CoalesceFieldDefinition.create(actorRecrod, "ActionGeo_Fullname", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(actorRecrod, "ActionGeo_CountryCode", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(actorRecrod, "ActionGeo_ADM1Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(actorRecrod, "ActionGeo_ADM2Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(actorRecrod, "ActionGeo_Lat", ECoalesceFieldDataTypes.FLOAT_TYPE);
		CoalesceFieldDefinition.create(actorRecrod, "ActionGeo_Long", ECoalesceFieldDataTypes.FLOAT_TYPE);
		CoalesceFieldDefinition.create(actorRecrod, "ActionGeo_FeatureID", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(actorRecrod, "DATEADDED", ECoalesceFieldDataTypes.INTEGER_TYPE);
		CoalesceFieldDefinition.create(actorRecrod, "SOURCEURL", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(actorRecrod, "DateTime", ECoalesceFieldDataTypes.DATE_TIME_TYPE);
		*/
		CoalesceFieldDefinition.create(actorRecordSet, "Actor1Geo_Location",
				ECoalesceFieldDataTypes.GEOCOORDINATE_TYPE);
		//CoalesceFieldDefinition.create(actorRecrod, "Actor2Geo_Location",
		//		ECoalesceFieldDataTypes.GEOCOORDINATE_TYPE);
		//CoalesceFieldDefinition.create(actorRecrod, "ActionGeo_Location",
		//		ECoalesceFieldDataTypes.GEOCOORDINATE_TYPE);
	
		CoalesceRecord eventRecord = actorRecordSet.addNew();
	
	}
}
