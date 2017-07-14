package com.incadencecorp.coalesce.ingester.gdelt;

import com.incadencecorp.coalesce.framework.datamodel.CoalesceFieldDefinition;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecord;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecordset;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceSection;
import com.incadencecorp.coalesce.framework.datamodel.ECoalesceFieldDataTypes;

public class EventRecord extends GDELTRecord {

    // Event Record fields
    private String GlobalEventID;
    private String Day;
    private String MonthYear;
    private String Year;
    private String FractionDate;
    private String IsRootEvent;
    private String EventCode;
    private String EventBaseCode;
    private String EventRootCode;
    private String QuadClass;
    private String GoldsteinScale;
    private String NumMentions;
    private String NumSources;
    private String NumArticles;
    private String AvgTone;
    private String ActionGeoType;
    private String ActionGeoFullname;
    private String ActionGeoCountryCode;
    private String ActionGeoADM1Code;
    private String ActionGeoADM2Code;
    private String ActionGeoFeatureID;
    private String DateAdded;
    private String SourceURL;
    private String DateTime;
    private String ActionGeoLocation;

    /**
     * @return the globalEventID
     */
    public String getGlobalEventID()
    {
        return GlobalEventID;
    }

    /**
     * @param globalEventID the globalEventID to set
     */
    public void setGlobalEventID(String globalEventID)
    {
        GlobalEventID = globalEventID;
    }

    /**
     * @return the day
     */
    public String getDay()
    {
        return Day;
    }

    /**
     * @param day the day to set
     */
    public void setDay(String day)
    {
        Day = day;
    }

    /**
     * @return the monthYear
     */
    public String getMonthYear()
    {
        return MonthYear;
    }

    /**
     * @param monthYear the monthYear to set
     */
    public void setMonthYear(String monthYear)
    {
        MonthYear = monthYear;
    }

    /**
     * @return the year
     */
    public String getYear()
    {
        return Year;
    }

    /**
     * @param year the year to set
     */
    public void setYear(String year)
    {
        Year = year;
    }

    /**
     * @return the fractionDate
     */
    public String getFractionDate()
    {
        return FractionDate;
    }

    /**
     * @param fractionDate the fractionDate to set
     */
    public void setFractionDate(String fractionDate)
    {
        FractionDate = fractionDate;
    }

    /**
     * @return the isRootEvent
     */
    public String getIsRootEvent()
    {
        return IsRootEvent;
    }

    /**
     * @param isRootEvent the isRootEvent to set
     */
    public void setIsRootEvent(String isRootEvent)
    {
        IsRootEvent = isRootEvent;
    }

    /**
     * @return the eventCode
     */
    public String getEventCode()
    {
        return EventCode;
    }

    /**
     * @param eventCode the eventCode to set
     */
    public void setEventCode(String eventCode)
    {
        EventCode = eventCode;
    }

    /**
     * @return the eventBaseCode
     */
    public String getEventBaseCode()
    {
        return EventBaseCode;
    }

    /**
     * @param eventBaseCode the eventBaseCode to set
     */
    public void setEventBaseCode(String eventBaseCode)
    {
        EventBaseCode = eventBaseCode;
    }

    /**
     * @return the eventRootCode
     */
    public String getEventRootCode()
    {
        return EventRootCode;
    }

    /**
     * @param eventRootCode the eventRootCode to set
     */
    public void setEventRootCode(String eventRootCode)
    {
        EventRootCode = eventRootCode;
    }

    /**
     * @return the quadClass
     */
    public String getQuadClass()
    {
        return QuadClass;
    }

    /**
     * @param quadClass the quadClass to set
     */
    public void setQuadClass(String quadClass)
    {
        QuadClass = quadClass;
    }

    /**
     * @return the goldsteinScale
     */
    public String getGoldsteinScale()
    {
        return GoldsteinScale;
    }

    /**
     * @param goldsteinScale the goldsteinScale to set
     */
    public void setGoldsteinScale(String goldsteinScale)
    {
        GoldsteinScale = goldsteinScale;
    }

    /**
     * @return the numMentions
     */
    public String getNumMentions()
    {
        return NumMentions;
    }

    /**
     * @param numMentions the numMentions to set
     */
    public void setNumMentions(String numMentions)
    {
        NumMentions = numMentions;
    }

    /**
     * @return the numSources
     */
    public String getNumSources()
    {
        return NumSources;
    }

    /**
     * @param numSources the numSources to set
     */
    public void setNumSources(String numSources)
    {
        NumSources = numSources;
    }

    /**
     * @return the numArticles
     */
    public String getNumArticles()
    {
        return NumArticles;
    }

    /**
     * @param numArticles the numArticles to set
     */
    public void setNumArticles(String numArticles)
    {
        NumArticles = numArticles;
    }

    /**
     * @return the avgTone
     */
    public String getAvgTone()
    {
        return AvgTone;
    }

    /**
     * @param avgTone the avgTone to set
     */
    public void setAvgTone(String avgTone)
    {
        AvgTone = avgTone;
    }

    /**
     * @return the actionGeoType
     */
    public String getActionGeoType()
    {
        return ActionGeoType;
    }

    /**
     * @param actionGeoType the actionGeoType to set
     */
    public void setActionGeoType(String actionGeoType)
    {
        ActionGeoType = actionGeoType;
    }

    /**
     * @return the actionGeoFullname
     */
    public String getActionGeoFullname()
    {
        return ActionGeoFullname;
    }

    /**
     * @param actionGeoFullname the actionGeoFullname to set
     */
    public void setActionGeoFullname(String actionGeoFullname)
    {
        ActionGeoFullname = actionGeoFullname;
    }

    /**
     * @return the actionGeoCountryCode
     */
    public String getActionGeoCountryCode()
    {
        return ActionGeoCountryCode;
    }

    /**
     * @param actionGeoCountryCode the actionGeoCountryCode to set
     */
    public void setActionGeoCountryCode(String actionGeoCountryCode)
    {
        ActionGeoCountryCode = actionGeoCountryCode;
    }

    /**
     * @return the actionGeoADM1Code
     */
    public String getActionGeoADM1Code()
    {
        return ActionGeoADM1Code;
    }

    /**
     * @param actionGeoADM1Code the actionGeoADM1Code to set
     */
    public void setActionGeoADM1Code(String actionGeoADM1Code)
    {
        ActionGeoADM1Code = actionGeoADM1Code;
    }

    /**
     * @return the actionGeoADM2Code
     */
    public String getActionGeoADM2Code()
    {
        return ActionGeoADM2Code;
    }

    /**
     * @param actionGeoADM2Code the actionGeoADM2Code to set
     */
    public void setActionGeoADM2Code(String actionGeoADM2Code)
    {
        ActionGeoADM2Code = actionGeoADM2Code;
    }

    /**
     * @return the actionGeoFeatureID
     */
    public String getActionGeoFeatureID()
    {
        return ActionGeoFeatureID;
    }

    /**
     * @param actionGeoFeatureID the actionGeoFeatureID to set
     */
    public void setActionGeoFeatureID(String actionGeoFeatureID)
    {
        ActionGeoFeatureID = actionGeoFeatureID;
    }

    /**
     * @return the dateAdded
     */
    public String getDateAdded()
    {
        return DateAdded;
    }

    /**
     * @param dateAdded the dateAdded to set
     */
    public void setDateAdded(String dateAdded)
    {
        DateAdded = dateAdded;
    }

    /**
     * @return the sourceURL
     */
    public String getSourceURL()
    {
        return SourceURL;
    }

    /**
     * @param sourceURL the sourceURL to set
     */
    public void setSourceURL(String sourceURL)
    {
        SourceURL = sourceURL;
    }

    /**
     * @return the dateTime
     */
    public String getDateTime()
    {
        return DateTime;
    }

    /**
     * @param dateTime the dateTime to set
     */
    public void setDateTime(String dateTime)
    {
        DateTime = dateTime;
    }

    /**
     * @return the actionGeoLocation
     */
    public String getActionGeoLocation()
    {
        return ActionGeoLocation;
    }

    /**
     * @param actionGeoLocation the actionGeoLocation to set
     */
    public void setActionGeoLocation(String actionGeoLocation)
    {
        ActionGeoLocation = actionGeoLocation;
    }

    public EventRecord()
    {

    }

    public EventRecord(CoalesceRecord record)
    {
        super(record);

    }

    public CoalesceRecordset createRecordSet(CoalesceSection section, String pathName)
    {
        CoalesceRecordset eventRecordSet = super.getRecordSet();

        CoalesceFieldDefinition.create(eventRecordSet, EventConstants.GlobalEventID, ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(eventRecordSet, EventConstants.Day, ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(eventRecordSet, EventConstants.MonthYear, ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(eventRecordSet, EventConstants.Year, ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(eventRecordSet, EventConstants.FractionDate, ECoalesceFieldDataTypes.FLOAT_TYPE);

        CoalesceFieldDefinition.create(eventRecordSet, EventConstants.IsRootEvent, ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(eventRecordSet, EventConstants.EventCode, ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(eventRecordSet, EventConstants.EventBaseCode, ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(eventRecordSet, EventConstants.EventRootCode, ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(eventRecordSet, EventConstants.QuadClass, ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(eventRecordSet, EventConstants.GoldsteinScale, ECoalesceFieldDataTypes.FLOAT_TYPE);
        CoalesceFieldDefinition.create(eventRecordSet, EventConstants.NumMentions, ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(eventRecordSet, EventConstants.NumSources, ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(eventRecordSet, EventConstants.NumArticles, ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(eventRecordSet, EventConstants.AvgTone, ECoalesceFieldDataTypes.FLOAT_TYPE);

        CoalesceFieldDefinition.create(eventRecordSet, EventConstants.ActionGeoType, ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(eventRecordSet,
                                       EventConstants.ActionGeoFullname,
                                       ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(eventRecordSet,
                                       EventConstants.ActionGeoCountryCode,
                                       ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(eventRecordSet,
                                       EventConstants.ActionGeoADM1Code,
                                       ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(eventRecordSet,
                                       EventConstants.ActionGeoADM2Code,
                                       ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(eventRecordSet,
                                       EventConstants.ActionGeoFeatureID,
                                       ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(eventRecordSet, EventConstants.DateAdded, ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(eventRecordSet, EventConstants.SourceURL, ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(eventRecordSet, EventConstants.DateTime, ECoalesceFieldDataTypes.DATE_TIME_TYPE);

        CoalesceFieldDefinition.create(eventRecordSet,
                                       EventConstants.ActionGeoLocation,
                                       ECoalesceFieldDataTypes.GEOCOORDINATE_TYPE);

        return eventRecordSet;
    }

}
