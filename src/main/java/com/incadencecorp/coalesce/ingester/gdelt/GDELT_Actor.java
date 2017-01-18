package com.incadencecorp.coalesce.ingester.gdelt;

import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecord;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecordset;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceSection;

//Setup actor base record set
// create OERepository, then Actor base on OERepository

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
		CoalesceRecordset actorRecordSet = ActorRecord.createRecordSet(actorSection, "Actor_Recordset");
	
		CoalesceRecord actorRecord = actorRecordSet.addNew();
	
	}
}
