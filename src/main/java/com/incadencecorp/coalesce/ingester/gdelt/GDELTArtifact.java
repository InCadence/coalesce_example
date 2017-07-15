package com.incadencecorp.coalesce.ingester.gdelt;

import java.io.File;
import java.io.IOException;

import org.xml.sax.SAXException;

import com.incadencecorp.coalesce.common.exceptions.CoalescePersistorException;
import com.incadencecorp.coalesce.framework.CoalesceFramework;
import com.incadencecorp.coalesce.framework.CoalesceObjectFactory;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceEntityTemplate;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceFieldDefinition;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecord;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecordset;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceSection;
import com.incadencecorp.coalesce.framework.datamodel.ECoalesceFieldDataTypes;

public class GDELTArtifact extends GenericArtifact {
	
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
		setName(GDELTArtifactConstants.Name);
		setSource(GDELTArtifactConstants.Source);
		setVersion(GDELTArtifactConstants.Version);
		setTitle(GDELTArtifactConstants.Title);
        setAttribute("classname", GDELTArtifact.class.getName());
        
		CoalesceSection gdeltArtifactSection = CoalesceSection.create(this, GDELTArtifactConstants.GDELTArtifactSection);
		CoalesceRecordset gdeltArtifactRecordset = GDELTArtifactRecord.createRecordSet(gdeltArtifactSection, GDELTArtifactConstants.GDELTArtifactRecordset);
		gdeltArtifactRecordset.addNew();

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
	    CoalesceRecordset artifactRecordSet = this.getCoalesceRecordsetForNamePath(GDELTEntityConstants.GDELTEntity + File.separator
	            + GDELTArtifactConstants.ArtifactSection + File.separator + GDELTEventConstants.EventRecordset);
	    return  (GDELTArtifactRecord) artifactRecordSet.getItem(record);
	    
	}
 

}

