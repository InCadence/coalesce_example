package com.incadencecorp.coalesce.ingester.gdelt;

public interface GDELTEventConstants {
	static final String Name = "GDELTEvent";
	static final String Source = "G2Core";
	static final String Version = "0.1";

	static final String Title  = "G2CoreEvent";
	static final String EventSection = "Event";
	static final String EventRecordset = "Event";
	
	static final String GlobalEventID = "GlobalEventID";
	static final String Day = "Day";
	static final String MonthYear = "MonthYear";
	static final String Year = "Year";
	static final String FractionDate = "FractionDate";
	
	static final String IsRootEvent = "IsRootEvent";
	static final String EventCode = "EventCode";
	static final String EventBaseCode = "EventBaseCode";
	static final String EventRootCode = "EventRootCode";
	static final String QuadClass = "QuadClass";
	static final String GoldsteinScale = "GoldsteinScale";
	static final String NumMentions = "NumMentions";
	static final String NumSources = "NumSources";
	static final String NumArticles = "NumArticles";
	static final String AvgTone = "AvgTone";

	static final String ActionGeoType = "ActionGeoType";
	static final String ActionGeoFullname = "ActionGeoFullname";
	static final String ActionGeoCountryCode = "ActionGeoCountryCode";
	static final String ActionGeoADM1Code = "ActionGeoADM1Code";
	static final String ActionGeoADM2Code = "ActionGeoADM2Code";
	static final String ActionGeoFeatureID = "ActionGeoFeatureID";
	static final String DateAdded = "DateAdded";
	static final String SourceURL = "SourceURL";
	static final String DateTime = "DateTime";
	
	static final String ActionGeoLocation = "ActionGeoLocation";
	static final String ActionGeo = "ActionGeo";

}
