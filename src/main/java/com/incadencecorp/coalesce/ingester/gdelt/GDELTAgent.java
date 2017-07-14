package com.incadencecorp.coalesce.ingester.gdelt;

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

    private static String NAME = GDELTAgentConstants.GDELTAgentName;
    private static String TITLE = GDELTAgentConstants.Title;
    private static String SOURCE = GDELTEntityConstants.Source;
    private static String VERSION = GDELTEntityConstants.Version;
	
    public static void registerEntity(CoalesceFramework framework) throws CoalescePersistorException, SAXException, IOException
	{
		CoalesceEntityTemplate template = framework.getCoalesceEntityTemplate(NAME, GDELTAgentConstants.Source,
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
}
