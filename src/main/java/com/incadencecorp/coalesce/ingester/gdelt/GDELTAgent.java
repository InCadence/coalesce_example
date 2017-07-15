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
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecordset;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceSection;

public class GDELTAgent extends CoalesceEntity {

	
    public static void registerEntity(CoalesceFramework framework) throws CoalescePersistorException, SAXException, IOException
	{
		CoalesceEntityTemplate template = framework.getCoalesceEntityTemplate(GDELTAgentConstants.Name, GDELTAgentConstants.Source,
				GDELTAgentConstants.Version);
		// Entity not registered, create template and register it.
		if (template == null) {
            GDELTAgent entity = new GDELTAgent();
            if (!entity.isInitialized())
            	entity.initialize();
            
            framework.saveCoalesceEntityTemplate(CoalesceEntityTemplate.create(entity));
		}
        CoalesceObjectFactory.register(GDELTAgent.class);
	}
    public GDELTAgent()
    {
    	initialize();
    }
    
    @Override
    public boolean initialize() {
    	if(!initializeEntity(GDELTAgentConstants.Name, GDELTAgentConstants.Source,
				GDELTAgentConstants.Version, "", "", GDELTAgentConstants.Title)) {
    		return false;
    	}
    	
    	return initializeReferences();
    }
    
    @Override
    public boolean initializeEntity(String name, String source, String version, String entityId, String entityIdType, String title) {
    	if(!super.initializeEntity(name, source, version, entityId, entityIdType, title)) {
			return false;
		}
        setAttribute("classname", GDELTAgent.class.getName());

        CoalesceLinkageSection.create(this);

        CoalesceSection agentSection = CoalesceSection.create(this, GDELTAgentConstants.AgentSection);
        CoalesceRecordset agentRecordSet = GDELTAgentRecord.createRecordset(agentSection, GDELTAgentConstants.AgentRecordset);

        agentRecordSet.addNew();
    	
    	return true;
    }
    
	public  static String getRecordSetName() {
		return GDELTAgentConstants.AgentRecordset;
	}

	public static  String getQueryName() {
		return GDELTAgentConstants.AgentRecordset;
	}
	

    public GDELTAgentRecord getRecord() {
        return getRecord(0);
    }
    
    public GDELTAgentRecord getRecord(int record) {
        CoalesceRecordset agentRecordSet = this.getCoalesceRecordsetForNamePath(GDELTAgentConstants.Name + File.separator
                + GDELTAgentConstants.AgentSection + File.separator + GDELTAgentConstants.AgentRecordset);
        return  (GDELTAgentRecord) agentRecordSet.getItem(record);
        
    }
}
