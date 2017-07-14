package com.incadencecorp.coalesce.ingester.gdelt;

import java.io.IOException;

import org.xml.sax.SAXException;

import com.incadencecorp.coalesce.common.exceptions.CoalescePersistorException;
import com.incadencecorp.coalesce.framework.CoalesceFramework;
import com.incadencecorp.coalesce.framework.CoalesceObjectFactory;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceEntityTemplate;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceFieldDefinition;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecordset;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceSection;
import com.incadencecorp.coalesce.framework.datamodel.ECoalesceFieldDataTypes;

public class GDELTArtifact extends GenericArtifact {
	private static String RECORDSET = GDELTConstants.GDELTArtifactRecordset;
	private static String SECTION = GDELTConstants.GDELTArtifactSection;
	
	public static void registerEntity(CoalesceFramework framework) throws CoalescePersistorException, SAXException, IOException
	{
		CoalesceEntityTemplate template = framework.getCoalesceEntityTemplate(GDELTConstants.GDELTArtifactName, GDELTConstants.Source,
				GDELTConstants.Version);
		// Entity not registered, create template and register it.
		if (template == null) {
            GDELTArtifact artifact = new GDELTArtifact();
           framework.saveCoalesceEntityTemplate(CoalesceEntityTemplate.create(artifact));           
		}
        CoalesceObjectFactory.register(GDELTArtifact.class);
	}
	public GDELTArtifact() {
		
		initialize();
		setName(GDELTConstants.GDELTArtifactName);
		setSource(GDELTConstants.Source);
		setVersion(VERSION);
		setTitle(GDELTConstants.Title);
        setAttribute("classname", GDELTArtifact.class.getName());
		
		CoalesceSection gdeltArtifactSection = CoalesceSection.create(this, GDELTConstants.GDELTArtifactSection);
		CoalesceRecordset gdeltArtifactRecordset = CoalesceRecordset.create(gdeltArtifactSection, GDELTConstants.GDELTArtifactRecordset);
		
		CoalesceFieldDefinition.create(gdeltArtifactRecordset, GDELTConstants.SourceFileName, ECoalesceFieldDataTypes.STRING_TYPE,false);
		CoalesceFieldDefinition.create(gdeltArtifactRecordset, GDELTConstants.GlobalEventID, ECoalesceFieldDataTypes.INTEGER_TYPE,false);
		CoalesceFieldDefinition.create(gdeltArtifactRecordset, GDELTConstants.RawText, ECoalesceFieldDataTypes.STRING_TYPE,true);
		
		gdeltArtifactRecordset.addNew();
	}
	public  static String getRecordSetName() {
		return RECORDSET;
	}

	public static  String getQueryName() {
		return RECORDSET;
	}
}
