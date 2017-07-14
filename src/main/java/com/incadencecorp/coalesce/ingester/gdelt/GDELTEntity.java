package com.incadencecorp.coalesce.ingester.gdelt;

import com.incadencecorp.coalesce.framework.datamodel.CoalesceEntity;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecordset;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceSection;

public class GDELTEntity extends CoalesceEntity {

	private static String NAME = GDELTEntityConstants.GDELTEntity;
	private static String TITLE = GDELTEntityConstants.Title;
	protected static String SOURCE = GDELTEntityConstants.Source;
	protected static String VERSION = GDELTEntityConstants.Version;
	private static String SECTION = GDELTEntityConstants.Section;
	private static  String RECORDSET = GDELTEntityConstants.Recordset;
	
	private GDELTRecord gDELTRecord ;

	public  static String getRecordSetName() {
		return RECORDSET;
	}

	public static  String getQueryName() {
		return RECORDSET;
	}

	public GDELTEntity() {
		
	}
	
	@Override
	public boolean initialize() {
		if (isInitialized()) {
			return true;
		}
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
	    setAttribute("classname", GDELTEntity.class.getName());
		
		CoalesceSection oeSection = CoalesceSection.create(this, GDELTEntityConstants.GDELTSection);
		CoalesceRecordset oeRecordset = gDELTRecord.createRecordSet(oeSection, GDELTEntityConstants.GDELTRecordset);
		
		oeRecordset.addNew();
		
		return true;
	}
}
