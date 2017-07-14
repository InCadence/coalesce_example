package com.incadencecorp.coalesce.ingester.gdelt;

public interface EventConstants {
	String GDELTEventName = "GDELTEvent";
    String Source = "";
    String Version = "0.1";

    String Title  = "G2CoreEvent";
	String EventSection = "EventSection";
	String EventRecordset = "EventRecordset";
	
	String GlobalEventID = "GlobalEventID";
	String Day = "Day";
	String MonthYear = "MonthYear";
	String Year = "Year";
	String FractionDate = "FractionDate";
	
	String IsRootEvent = "IsRootEvent";
	String EventCode = "EventCode";
	String EventBaseCode = "EventBaseCode";
	String EventRootCode = "EventRootCode";
	String QuadClass = "QuadClass";
	String GoldsteinScale = "GoldsteinScale";
	String NumMentions = "NumMentions";
	String NumSources = "NumSources";
	String NumArticles = "NumArticles";
	String AvgTone = "AvgTone";

	String ActionGeoType = "ActionGeoType";
	String ActionGeoFullname = "ActionGeoFullname";
	String ActionGeoCountryCode = "ActionGeoCountryCode";
	String ActionGeoADM1Code = "ActionGeoADM1Code";
	String ActionGeoADM2Code = "ActionGeoADM2Code";
	String ActionGeoFeatureID = "ActionGeoFeatureID";
	String DateAdded = "DateAdded";
	String SourceURL = "SourceURL";
	String DateTime = "DateTime";
	
	String ActionGeoLocation = "ActionGeoLocation";
	String ActionGeo = "ActionGeo";

}
