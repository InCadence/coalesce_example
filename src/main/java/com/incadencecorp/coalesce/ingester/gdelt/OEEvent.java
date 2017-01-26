package com.incadencecorp.coalesce.ingester.gdelt;

import com.incadencecorp.coalesce.framework.datamodel.CoalesceLinkageSection;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecord;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecordset;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceSection;

public class OEEvent extends OEEntity {
	
	private static String NAME = "OEEvent";
	private static String TITLE = "G2CoreEvent";
	
	public OEEvent() {
		initialize();
		setName(NAME);
		setVersion(VERSION);
		setTitle(TITLE);
	
		CoalesceLinkageSection.create(this);
		
		CoalesceSection eventSection = CoalesceSection.create(this, "EventSection");
		CoalesceRecordset eventRecordSet = EventRecord.createRecordSet(eventSection, "EventRecordset");

		eventRecordSet.addNew();
	}
}
