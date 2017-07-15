package com.incadencecorp.coalesce.ingester.gdelt;

import java.io.File;
import java.io.IOException;

import org.xml.sax.SAXException;

import com.incadencecorp.coalesce.common.exceptions.CoalescePersistorException;
import com.incadencecorp.coalesce.framework.CoalesceFramework;
import com.incadencecorp.coalesce.framework.CoalesceObjectFactory;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceEntity;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceEntityTemplate;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceFieldDefinition;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceLinkageSection;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecord;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecordset;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceSection;
import com.incadencecorp.coalesce.framework.datamodel.ECoalesceFieldDataTypes;

public class GDELTArtifact extends CoalesceEntity {
	
	public static void registerEntity(CoalesceFramework framework) throws CoalescePersistorException, SAXException, IOException
	{
		CoalesceEntityTemplate template = framework.getCoalesceEntityTemplate(GDELTArtifactConstants.Name, GDELTArtifactConstants.Source,
				GDELTArtifactConstants.Version);
		// Entity not registered, create template and register it.
		if (template == null) {
            GDELTArtifact artifact = new GDELTArtifact();
           framework.saveCoalesceEntityTemplate(CoalesceEntityTemplate.create(artifact));           
		}
        CoalesceObjectFactory.register(GDELTArtifact.class);
	}
	public GDELTArtifact() {
		
		initialize();
		
	}
    @Override
    public boolean initialize() {
    	if(!initializeEntity(GDELTArtifactConstants.Name, GDELTArtifactConstants.Source,
    			GDELTArtifactConstants.Version, "", "", GDELTAgentConstants.Title)) {
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

        CoalesceSection agentSection = CoalesceSection.create(this, GDELTArtifactConstants.ArtifactSection);
        CoalesceRecordset agentRecordSet = GDELTArtifactRecord.createRecordset(agentSection, GDELTArtifactConstants.ArtifactRecordset);

        agentRecordSet.addNew();
    	
    	return true;
    }
	public  static String getRecordSetName() {
		return GDELTArtifactConstants.ArtifactRecordset;
	}

	public static  String getQueryName() {
		return GDELTArtifactConstants.ArtifactRecordset;
	}
	public GDELTArtifactRecord getRecord() {
		return getRecord(0);
	}
	
	public GDELTArtifactRecord getRecord(int record) {
	    CoalesceRecordset artifactRecordSet = this.getCoalesceRecordsetForNamePath(GDELTArtifactConstants.Name + File.separator
	            + GDELTArtifactConstants.ArtifactSection + File.separator + GDELTEventConstants.EventRecordset);
	    return  (GDELTArtifactRecord) artifactRecordSet.getItem(record);
	    
	}
 

}

