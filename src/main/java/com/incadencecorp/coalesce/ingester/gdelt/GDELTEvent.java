package com.incadencecorp.coalesce.ingester.gdelt;

import java.io.File;
import java.io.IOException;

import org.xml.sax.SAXException;

import com.incadencecorp.coalesce.common.exceptions.CoalescePersistorException;
import com.incadencecorp.coalesce.framework.CoalesceFramework;
import com.incadencecorp.coalesce.framework.CoalesceObjectFactory;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceEntity;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceEntityTemplate;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceLinkageSection;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecord;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecordset;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceSection;


public class GDELTEvent extends CoalesceEntity  {
	
	public static void registerEntity(CoalesceFramework framework) throws CoalescePersistorException, SAXException, IOException
    {
		CoalesceEntityTemplate template = framework.getCoalesceEntityTemplate(GDELTEventConstants.Name, GDELTEventConstants.Source,
				GDELTEventConstants.Version);
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

	}
	
	@Override
	public boolean initialize() {
		if(!initializeEntity(GDELTEventConstants.Name, GDELTEventConstants.Source,
				GDELTEventConstants.Version, "", "", GDELTEventConstants.Title)) {
			return false;
		}
		
		return initializeReferences();
	}
    
    @Override
    public boolean initializeEntity(String name, String source, String version, String entityId, String entityIdType, String title) {
    	if(!super.initializeEntity(name, source, version, entityId, entityIdType, title)) {
			return false;
		}
        setAttribute("classname", GDELTEvent.class.getName());
	
		CoalesceLinkageSection.create(this);
		
		CoalesceSection eventSection = CoalesceSection.create(this, GDELTEventConstants.EventSection);
		CoalesceRecordset eventRecordSet = GDELTEventRecord.createRecordSet(eventSection, GDELTEventConstants.EventRecordset);

		eventRecordSet.addNew();
    	
    	return true;
    }
	
	public  static String getRecordSetName() {
		return GDELTEventConstants.EventRecordset;
	}

	public static  String getQueryName() {
		return GDELTEventConstants.EventRecordset;
	}
	
	public GDELTEventRecord getRecord() {
		return getRecord(0);
	}
	
	public GDELTEventRecord getRecord(int record) {
	    CoalesceRecordset eventRecordSet = this.getCoalesceRecordsetForNamePath(GDELTEntityConstants.GDELTEntity + File.separator
	            + GDELTEventConstants.EventSection + File.separator + GDELTEventConstants.EventRecordset);
	    return  (GDELTEventRecord) eventRecordSet.getItem(record);
	    
	}
 

}
