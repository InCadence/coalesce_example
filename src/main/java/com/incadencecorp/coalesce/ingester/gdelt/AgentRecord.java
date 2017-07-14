package com.incadencecorp.coalesce.ingester.gdelt;

import com.incadencecorp.coalesce.framework.datamodel.CoalesceFieldDefinition;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecord;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecordset;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceSection;
import com.incadencecorp.coalesce.framework.datamodel.ECoalesceFieldDataTypes;

public class AgentRecord extends GDELTRecord {

    // Agent Record fields
    private String AgentCode;
    private String AgentName;
    private String NameMetaphone;
    private String AgentCountryCode;
    private String AgentKnownGroupCode;
    private String AgentEthnicCode;
    private String AgentReligion1Code;
    private String AgentReligion2Code;
    private String AgentType1Code;
    private String AgentType2Code;
    private String AgentType3Code;
    private String AgentGeoType;
    private String AgentGeoFullname;
    private String AgentGeoCountryCode;
    private String AgentGeoADM1Code;
    private String AgentGeoADM2Code;
    private String AgentGeoFeatureID;

    /**
     * @return the agentCode
     */
    public String getAgentCode()
    {
        return AgentCode;
    }

    /**
     * @param agentCode the agentCode to set
     */
    public void setAgentCode(String agentCode)
    {
        AgentCode = agentCode;
    }

    /**
     * @return the agentName
     */
    public String getAgentName()
    {
        return AgentName;
    }

    /**
     * @param agentName the agentName to set
     */
    public void setAgentName(String agentName)
    {
        AgentName = agentName;
    }

    /**
     * @return the nameMetaphone
     */
    public String getNameMetaphone()
    {
        return NameMetaphone;
    }

    /**
     * @param nameMetaphone the nameMetaphone to set
     */
    public void setNameMetaphone(String nameMetaphone)
    {
        NameMetaphone = nameMetaphone;
    }

    /**
     * @return the agentCountryCode
     */
    public String getAgentCountryCode()
    {
        return AgentCountryCode;
    }

    /**
     * @param agentCountryCode the agentCountryCode to set
     */
    public void setAgentCountryCode(String agentCountryCode)
    {
        AgentCountryCode = agentCountryCode;
    }

    /**
     * @return the agentKnownGroupCode
     */
    public String getAgentKnownGroupCode()
    {
        return AgentKnownGroupCode;
    }

    /**
     * @param agentKnownGroupCode the agentKnownGroupCode to set
     */
    public void setAgentKnownGroupCode(String agentKnownGroupCode)
    {
        AgentKnownGroupCode = agentKnownGroupCode;
    }

    /**
     * @return the agentEthnicCode
     */
    public String getAgentEthnicCode()
    {
        return AgentEthnicCode;
    }

    /**
     * @param agentEthnicCode the agentEthnicCode to set
     */
    public void setAgentEthnicCode(String agentEthnicCode)
    {
        AgentEthnicCode = agentEthnicCode;
    }

    /**
     * @return the agentReligion1Code
     */
    public String getAgentReligion1Code()
    {
        return AgentReligion1Code;
    }

    /**
     * @param agentReligion1Code the agentReligion1Code to set
     */
    public void setAgentReligion1Code(String agentReligion1Code)
    {
        AgentReligion1Code = agentReligion1Code;
    }

    /**
     * @return the agentReligion2Code
     */
    public String getAgentReligion2Code()
    {
        return AgentReligion2Code;
    }

    /**
     * @param agentReligion2Code the agentReligion2Code to set
     */
    public void setAgentReligion2Code(String agentReligion2Code)
    {
        AgentReligion2Code = agentReligion2Code;
    }

    /**
     * @return the agentType1Code
     */
    public String getAgentType1Code()
    {
        return AgentType1Code;
    }

    /**
     * @param agentType1Code the agentType1Code to set
     */
    public void setAgentType1Code(String agentType1Code)
    {
        AgentType1Code = agentType1Code;
    }

    /**
     * @return the agentType2Code
     */
    public String getAgentType2Code()
    {
        return AgentType2Code;
    }

    /**
     * @param agentType2Code the agentType2Code to set
     */
    public void setAgentType2Code(String agentType2Code)
    {
        AgentType2Code = agentType2Code;
    }

    /**
     * @return the agentType3Code
     */
    public String getAgentType3Code()
    {
        return AgentType3Code;
    }

    /**
     * @param agentType3Code the agentType3Code to set
     */
    public void setAgentType3Code(String agentType3Code)
    {
        AgentType3Code = agentType3Code;
    }

    /**
     * @return the agentGeoType
     */
    public String getAgentGeoType()
    {
        return AgentGeoType;
    }

    /**
     * @param agentGeoType the agentGeoType to set
     */
    public void setAgentGeoType(String agentGeoType)
    {
        AgentGeoType = agentGeoType;
    }

    /**
     * @return the agentGeoFullname
     */
    public String getAgentGeoFullname()
    {
        return AgentGeoFullname;
    }

    /**
     * @param agentGeoFullname the agentGeoFullname to set
     */
    public void setAgentGeoFullname(String agentGeoFullname)
    {
        AgentGeoFullname = agentGeoFullname;
    }

    /**
     * @return the agentGeoCountryCode
     */
    public String getAgentGeoCountryCode()
    {
        return AgentGeoCountryCode;
    }

    /**
     * @param agentGeoCountryCode the agentGeoCountryCode to set
     */
    public void setAgentGeoCountryCode(String agentGeoCountryCode)
    {
        AgentGeoCountryCode = agentGeoCountryCode;
    }

    /**
     * @return the agentGeoADM1Code
     */
    public String getAgentGeoADM1Code()
    {
        return AgentGeoADM1Code;
    }

    /**
     * @param agentGeoADM1Code the agentGeoADM1Code to set
     */
    public void setAgentGeoADM1Code(String agentGeoADM1Code)
    {
        AgentGeoADM1Code = agentGeoADM1Code;
    }

    /**
     * @return the agentGeoADM2Code
     */
    public String getAgentGeoADM2Code()
    {
        return AgentGeoADM2Code;
    }

    /**
     * @param agentGeoADM2Code the agentGeoADM2Code to set
     */
    public void setAgentGeoADM2Code(String agentGeoADM2Code)
    {
        AgentGeoADM2Code = agentGeoADM2Code;
    }

    /**
     * @return the agentGeoFeatureID
     */
    public String getAgentGeoFeatureID()
    {
        return AgentGeoFeatureID;
    }

    /**
     * @param agentGeoFeatureID the agentGeoFeatureID to set
     */
    public void setAgentGeoFeatureID(String agentGeoFeatureID)
    {
        AgentGeoFeatureID = agentGeoFeatureID;
    }

    public static CoalesceRecordset createRecordset(CoalesceSection section, String pathName)
    {
        CoalesceRecordset recordSet = CoalesceRecordset.create(section, pathName);
        CoalesceFieldDefinition.create(recordSet, AgentConstants.AgentCode, ECoalesceFieldDataTypes.STRING_TYPE, false);
        CoalesceFieldDefinition.create(recordSet, AgentConstants.AgentName, ECoalesceFieldDataTypes.STRING_TYPE, false);
        CoalesceFieldDefinition.create(recordSet, AgentConstants.NameMetaphone, ECoalesceFieldDataTypes.STRING_TYPE, true);
        CoalesceFieldDefinition.create(recordSet,
                                       AgentConstants.AgentCountryCode,
                                       ECoalesceFieldDataTypes.STRING_TYPE,
                                       false);
        CoalesceFieldDefinition.create(recordSet,
                                       AgentConstants.AgentKnownGroupCode,
                                       ECoalesceFieldDataTypes.STRING_TYPE,
                                       true);
        CoalesceFieldDefinition.create(recordSet, AgentConstants.AgentEthnicCode, ECoalesceFieldDataTypes.STRING_TYPE, true);
        CoalesceFieldDefinition.create(recordSet,
                                       AgentConstants.AgentReligion1Code,
                                       ECoalesceFieldDataTypes.STRING_TYPE,
                                       true);
        CoalesceFieldDefinition.create(recordSet,
                                       AgentConstants.AgentReligion2Code,
                                       ECoalesceFieldDataTypes.STRING_TYPE,
                                       true);
        CoalesceFieldDefinition.create(recordSet, AgentConstants.AgentType1Code, ECoalesceFieldDataTypes.STRING_TYPE, true);
        CoalesceFieldDefinition.create(recordSet, AgentConstants.AgentType2Code, ECoalesceFieldDataTypes.STRING_TYPE, true);
        CoalesceFieldDefinition.create(recordSet, AgentConstants.AgentType3Code, ECoalesceFieldDataTypes.STRING_TYPE, true);

        CoalesceFieldDefinition.create(recordSet, AgentConstants.AgentGeoType, ECoalesceFieldDataTypes.INTEGER_TYPE, true);
        CoalesceFieldDefinition.create(recordSet,
                                       AgentConstants.AgentGeoFullname,
                                       ECoalesceFieldDataTypes.STRING_TYPE,
                                       true);
        CoalesceFieldDefinition.create(recordSet,
                                       AgentConstants.AgentGeoCountryCode,
                                       ECoalesceFieldDataTypes.STRING_TYPE,
                                       true);
        CoalesceFieldDefinition.create(recordSet,
                                       AgentConstants.AgentGeoADM1Code,
                                       ECoalesceFieldDataTypes.STRING_TYPE,
                                       true);
        CoalesceFieldDefinition.create(recordSet,
                                       AgentConstants.AgentGeoADM2Code,
                                       ECoalesceFieldDataTypes.STRING_TYPE,
                                       true);
        CoalesceFieldDefinition.create(recordSet,
                                       AgentConstants.AgentGeoFeatureID,
                                       ECoalesceFieldDataTypes.STRING_TYPE,
                                       true);

        CoalesceFieldDefinition.create(recordSet,
                                       AgentConstants.AgentGeoLocation,
                                       ECoalesceFieldDataTypes.GEOCOORDINATE_TYPE);

        return recordSet;
    }

}
