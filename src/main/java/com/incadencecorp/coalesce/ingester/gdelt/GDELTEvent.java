package com.incadencecorp.coalesce.ingester.gdelt;

import java.io.IOException;

import org.xml.sax.SAXException;

import com.incadencecorp.coalesce.common.exceptions.CoalescePersistorException;
import com.incadencecorp.coalesce.framework.CoalesceFramework;
import com.incadencecorp.coalesce.framework.CoalesceObjectFactory;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceEntityTemplate;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceLinkageSection;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecordset;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceSection;

public class GDELTEvent extends GDELTEntity {
	
	private static String NAME = EventConstants.GDELTEventName;
	private static String TITLE = EventConstants.Title;
	private GDELTRecord gdeltRecord;
    public static void registerEntity(CoalesceFramework framework) throws CoalescePersistorException, SAXException, IOException
    {
		CoalesceEntityTemplate template = framework.getCoalesceEntityTemplate(NAME, EventConstants.Source,
				EventConstants.Version);
		// Entity not registered, create template and register it.
		if (template == null) {
            GDELTEvent entity = new GDELTEvent();
            if (!entity.isInitialized())
            	entity.initialize();
            framework.saveCoalesceEntityTemplate(CoalesceEntityTemplate.create(entity));
		}
        CoalesceObjectFactory.register(GDELTEvent.class);
	}	
    
	public GDELTEvent() {
		initialize();
	}
	
	@Override
	public boolean initialize() {
		if(!initializeEntity(NAME, SOURCE, VERSION, "", "", TITLE)) {
			return false;
		}
		
		return initializeReferences();
	}
    
    @Override
    public boolean initializeEntity(String name, String source, String version, String entityId, String entityIdType, String title) {
    	if(!super.initializeEntity(name, source, version, entityId, entityIdType, title)) {
			return false;
		}
    	gdeltRecord = new EventRecord();
        setAttribute("classname", GDELTEvent.class.getName());
	
		CoalesceLinkageSection.create(this);
		
		CoalesceSection eventSection = CoalesceSection.create(this, EventConstants.EventSection);
		CoalesceRecordset eventRecordSet = gdeltRecord.createRecordSet(eventSection, EventConstants.EventRecordset);

		eventRecordSet.addNew();
    	
    	return true;
    }
	
	public  static String getRecordSetName() {
		return EventConstants.EventRecordset;
	}

	public static  String getQueryName() {
		return EventConstants.EventRecordset;
	}
}
