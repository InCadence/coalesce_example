package com.incadencecorp.coalesce.ingester.gdelt;

import com.incadencecorp.coalesce.framework.datamodel.CoalesceLinkageSection;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecord;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecordset;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceSection;


public class OEActor extends OEEntity {

	private static String NAME = "OEActor";
	private static String TITLE = "G2CoreActor";
	
	public OEActor() {
		initialize();
		setName(NAME);
		setVersion(VERSION);
		setTitle(TITLE);
	
		CoalesceLinkageSection.create(this);
		
		CoalesceSection actorSection = CoalesceSection.create(this, "ActorSection");
		CoalesceRecordset actorRecordSet = ActorRecord.createRecordset(actorSection, "ActorRecordset");
	
		actorRecordSet.addNew();
	}
}
