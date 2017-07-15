/**
 * 
 */
package com.incadencecorp.coalesce.ingester.gdelt;

import com.incadencecorp.coalesce.framework.datamodel.CoalesceFieldDefinition;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecord;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecordset;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceSection;
import com.incadencecorp.coalesce.framework.datamodel.ECoalesceFieldDataTypes;

public class GDELTRecord extends CoalesceRecord {

    private CoalesceRecordset recordSet;

    private String DataSource;
    private String OntologyReference;
    private String IsSimulation;
    private String PMESIIPTPolitical;
    private String PMESIIPTMilitary;
    private String PMESIIPTEconomic;
    private String PMESIIPTSocial;
    private String PMESIIPTInformation;
    private String PMESIIPTInfrastructure;
    private String PMESIIPTPhysicalEnvironment;
    private String PMESIIPTTime;
    private String Tags;


    /**
     * @return the recordSet
     */
    public CoalesceRecordset getRecordSet()
    {
        return recordSet;
    }

    /**
     * @param recordSet the recordSet to set
     */
    public void setRecordSet(CoalesceRecordset recordSet)
    {
        this.recordSet = recordSet;
    }

    /**
     * @return the dataSource
     */
    public String getDataSource()
    {
        return DataSource;
    }

    /**
     * @param dataSource the dataSource to set
     */
    public void setDataSource(String dataSource)
    {
        DataSource = dataSource;
    }

    /**
     * @return the ontologyReference
     */
    public String getOntologyReference()
    {
        return OntologyReference;
    }

    /**
     * @param ontologyReference the ontologyReference to set
     */
    public void setOntologyReference(String ontologyReference)
    {
        OntologyReference = ontologyReference;
    }

    /**
     * @return the isSimulation
     */
    public String getIsSimulation()
    {
        return IsSimulation;
    }

    /**
     * @param isSimulation the isSimulation to set
     */
    public void setIsSimulation(String isSimulation)
    {
        IsSimulation = isSimulation;
    }

    /**
     * @return the pMESIIPTPolitical
     */
    public String getPMESIIPTPolitical()
    {
        return PMESIIPTPolitical;
    }

    /**
     * @param pMESIIPTPolitical the pMESIIPTPolitical to set
     */
    public void setPMESIIPTPolitical(String pMESIIPTPolitical)
    {
        PMESIIPTPolitical = pMESIIPTPolitical;
    }

    /**
     * @return the pMESIIPTMilitary
     */
    public String getPMESIIPTMilitary()
    {
        return PMESIIPTMilitary;
    }

    /**
     * @param pMESIIPTMilitary the pMESIIPTMilitary to set
     */
    public void setPMESIIPTMilitary(String pMESIIPTMilitary)
    {
        PMESIIPTMilitary = pMESIIPTMilitary;
    }

    /**
     * @return the pMESIIPTEconomic
     */
    public String getPMESIIPTEconomic()
    {
        return PMESIIPTEconomic;
    }

    /**
     * @param pMESIIPTEconomic the pMESIIPTEconomic to set
     */
    public void setPMESIIPTEconomic(String pMESIIPTEconomic)
    {
        PMESIIPTEconomic = pMESIIPTEconomic;
    }

    /**
     * @return the pMESIIPTSocial
     */
    public String getPMESIIPTSocial()
    {
        return PMESIIPTSocial;
    }

    /**
     * @param pMESIIPTSocial the pMESIIPTSocial to set
     */
    public void setPMESIIPTSocial(String pMESIIPTSocial)
    {
        PMESIIPTSocial = pMESIIPTSocial;
    }

    /**
     * @return the pMESIIPTInformation
     */
    public String getPMESIIPTInformation()
    {
        return PMESIIPTInformation;
    }

    /**
     * @param pMESIIPTInformation the pMESIIPTInformation to set
     */
    public void setPMESIIPTInformation(String pMESIIPTInformation)
    {
        PMESIIPTInformation = pMESIIPTInformation;
    }

    /**
     * @return the pMESIIPTInfrastructure
     */
    public String getPMESIIPTInfrastructure()
    {
        return PMESIIPTInfrastructure;
    }

    /**
     * @param pMESIIPTInfrastructure the pMESIIPTInfrastructure to set
     */
    public void setPMESIIPTInfrastructure(String pMESIIPTInfrastructure)
    {
        PMESIIPTInfrastructure = pMESIIPTInfrastructure;
    }

    /**
     * @return the pMESIIPTPhysicalEnvironment
     */
    public String getPMESIIPTPhysicalEnvironment()
    {
        return PMESIIPTPhysicalEnvironment;
    }

    /**
     * @param pMESIIPTPhysicalEnvironment the pMESIIPTPhysicalEnvironment to set
     */
    public void setPMESIIPTPhysicalEnvironment(String pMESIIPTPhysicalEnvironment)
    {
        PMESIIPTPhysicalEnvironment = pMESIIPTPhysicalEnvironment;
    }

    /**
     * @return the pMESIIPTTime
     */
    public String getPMESIIPTTime()
    {
        return PMESIIPTTime;
    }

    /**
     * @param pMESIIPTTime the pMESIIPTTime to set
     */
    public void setPMESIIPTTime(String pMESIIPTTime)
    {
        PMESIIPTTime = pMESIIPTTime;
    }

    /**
     * @return the tags
     */
    public String getTags()
    {
        return Tags;
    }

    /**
     * @param tags the tags to set
     */
    public void setTags(String tags)
    {
        Tags = tags;
    }

    /**
     * 
     */
    public GDELTRecord()
    {

    }

    /**
     * @param record
     */
    public GDELTRecord(CoalesceRecord record)
    {
        super(record);
    }

    public CoalesceRecordset createRecordSet(CoalesceSection section, String pathName)
    {
        recordSet = CoalesceRecordset.create(section, pathName);

        CoalesceFieldDefinition.create(recordSet, GDELTEventConstants.DataSource, ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(recordSet, GDELTEventConstants.OntologyReference, ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(recordSet, GDELTEventConstants.IsSimulation, ECoalesceFieldDataTypes.BOOLEAN_TYPE);
        CoalesceFieldDefinition.create(recordSet, GDELTEventConstants.PMESIIPTPolitical, ECoalesceFieldDataTypes.FLOAT_TYPE);
        CoalesceFieldDefinition.create(recordSet, GDELTEventConstants.PMESIIPTMilitary, ECoalesceFieldDataTypes.FLOAT_TYPE);
        CoalesceFieldDefinition.create(recordSet, GDELTEventConstants.PMESIIPTEconomic, ECoalesceFieldDataTypes.FLOAT_TYPE);
        CoalesceFieldDefinition.create(recordSet, GDELTEventConstants.PMESIIPTSocial, ECoalesceFieldDataTypes.FLOAT_TYPE);
        CoalesceFieldDefinition.create(recordSet, GDELTEventConstants.PMESIIPTInformation, ECoalesceFieldDataTypes.FLOAT_TYPE);
        CoalesceFieldDefinition.create(recordSet, GDELTEventConstants.PMESIIPTInfrastructure, ECoalesceFieldDataTypes.FLOAT_TYPE);
        CoalesceFieldDefinition.create(recordSet, GDELTEventConstants.PMESIIPTPhysicalEnvironment, ECoalesceFieldDataTypes.FLOAT_TYPE);
        CoalesceFieldDefinition.create(recordSet, GDELTEventConstants.PMESIIPTTime, ECoalesceFieldDataTypes.FLOAT_TYPE);
        CoalesceFieldDefinition.create(recordSet, GDELTEventConstants.Tags, ECoalesceFieldDataTypes.STRING_LIST_TYPE);

        // event record setup
        CoalesceFieldDefinition.create(recordSet, GDELTEventConstants.GlobalEventID, ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(recordSet, GDELTEventConstants.Day, ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(recordSet, GDELTEventConstants.MonthYear, ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(recordSet, GDELTEventConstants.Year, ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(recordSet, GDELTEventConstants.FractionDate, ECoalesceFieldDataTypes.FLOAT_TYPE);

        CoalesceFieldDefinition.create(recordSet, GDELTEventConstants.IsRootEvent, ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(recordSet, GDELTEventConstants.EventCode, ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(recordSet, GDELTEventConstants.EventBaseCode, ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(recordSet, GDELTEventConstants.EventRootCode, ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(recordSet, GDELTEventConstants.QuadClass, ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(recordSet, GDELTEventConstants.GoldsteinScale, ECoalesceFieldDataTypes.FLOAT_TYPE);
        CoalesceFieldDefinition.create(recordSet, GDELTEventConstants.NumMentions, ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(recordSet, GDELTEventConstants.NumSources, ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(recordSet, GDELTEventConstants.NumArticles, ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(recordSet, GDELTEventConstants.AvgTone, ECoalesceFieldDataTypes.FLOAT_TYPE);

        CoalesceFieldDefinition.create(recordSet, GDELTEventConstants.ActionGeoType, ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(recordSet, GDELTEventConstants.ActionGeoFullname, ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(recordSet, GDELTEventConstants.ActionGeoCountryCode, ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(recordSet, GDELTEventConstants.ActionGeoADM1Code, ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(recordSet, GDELTEventConstants.ActionGeoADM2Code, ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(recordSet, GDELTEventConstants.ActionGeoFeatureID, ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(recordSet, GDELTEventConstants.DateAdded, ECoalesceFieldDataTypes.DATE_TIME_TYPE);
        CoalesceFieldDefinition.create(recordSet, GDELTEventConstants.SourceURL, ECoalesceFieldDataTypes.STRING_TYPE);

        CoalesceFieldDefinition.create(recordSet,
                                       GDELTEventConstants.ActionGeoLocation,
                                       ECoalesceFieldDataTypes.GEOCOORDINATE_TYPE);

        // agent record setup
        CoalesceFieldDefinition.create(recordSet, GDELTAgentConstants.AgentCode, ECoalesceFieldDataTypes.STRING_TYPE, false);
        CoalesceFieldDefinition.create(recordSet, GDELTAgentConstants.AgentName, ECoalesceFieldDataTypes.STRING_TYPE, false);
        CoalesceFieldDefinition.create(recordSet, GDELTAgentConstants.NameMetaphone, ECoalesceFieldDataTypes.STRING_TYPE, true);
        CoalesceFieldDefinition.create(recordSet,
                                       GDELTAgentConstants.AgentCountryCode,
                                       ECoalesceFieldDataTypes.STRING_TYPE,
                                       false);
        CoalesceFieldDefinition.create(recordSet,
                                       GDELTAgentConstants.AgentKnownGroupCode,
                                       ECoalesceFieldDataTypes.STRING_TYPE,
                                       true);
        CoalesceFieldDefinition.create(recordSet, GDELTAgentConstants.AgentEthnicCode, ECoalesceFieldDataTypes.STRING_TYPE, true);
        CoalesceFieldDefinition.create(recordSet,
                                       GDELTAgentConstants.AgentReligion1Code,
                                       ECoalesceFieldDataTypes.STRING_TYPE,
                                       true);
        CoalesceFieldDefinition.create(recordSet,
                                       GDELTAgentConstants.AgentReligion2Code,
                                       ECoalesceFieldDataTypes.STRING_TYPE,
                                       true);
        CoalesceFieldDefinition.create(recordSet, GDELTAgentConstants.AgentType1Code, ECoalesceFieldDataTypes.STRING_TYPE, true);
        CoalesceFieldDefinition.create(recordSet, GDELTAgentConstants.AgentType2Code, ECoalesceFieldDataTypes.STRING_TYPE, true);
        CoalesceFieldDefinition.create(recordSet, GDELTAgentConstants.AgentType3Code, ECoalesceFieldDataTypes.STRING_TYPE, true);

        CoalesceFieldDefinition.create(recordSet, GDELTAgentConstants.AgentGeoType, ECoalesceFieldDataTypes.INTEGER_TYPE, true);
        CoalesceFieldDefinition.create(recordSet,
                                       GDELTAgentConstants.AgentGeoFullname,
                                       ECoalesceFieldDataTypes.STRING_TYPE,
                                       true);
        CoalesceFieldDefinition.create(recordSet,
                                       GDELTAgentConstants.AgentGeoCountryCode,
                                       ECoalesceFieldDataTypes.STRING_TYPE,
                                       true);
        CoalesceFieldDefinition.create(recordSet,
                                       GDELTAgentConstants.AgentGeoADM1Code,
                                       ECoalesceFieldDataTypes.STRING_TYPE,
                                       true);
        CoalesceFieldDefinition.create(recordSet,
                                       GDELTAgentConstants.AgentGeoADM2Code,
                                       ECoalesceFieldDataTypes.STRING_TYPE,
                                       true);
        CoalesceFieldDefinition.create(recordSet,
                                       GDELTAgentConstants.AgentGeoFeatureID,
                                       ECoalesceFieldDataTypes.STRING_TYPE,
                                       true);

        CoalesceFieldDefinition.create(recordSet,
                                       GDELTAgentConstants.AgentGeoLocation,
                                       ECoalesceFieldDataTypes.GEOCOORDINATE_TYPE);

        return recordSet;
    }
}
