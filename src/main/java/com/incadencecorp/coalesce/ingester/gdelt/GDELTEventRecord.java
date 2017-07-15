package com.incadencecorp.coalesce.ingester.gdelt;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.incadencecorp.coalesce.common.exceptions.CoalesceDataFormatException;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceBooleanField;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceCoordinateField;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceDateTimeField;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceFieldDefinition;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceFloatField;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceIntegerField;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecord;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecordset;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceSection;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceStringField;
import com.incadencecorp.coalesce.framework.datamodel.ECoalesceFieldDataTypes;
import com.vividsolutions.jts.geom.Coordinate;

public class GDELTEventRecord extends CoalesceRecord {

    private final static Logger LOGGER = LoggerFactory.getLogger(GDELTEventRecord.class);

    public GDELTEventRecord()
    {

    }

    public GDELTEventRecord(CoalesceRecord record)
    {
        super(record);

    }

    public static CoalesceRecordset createRecordSet(CoalesceSection section, String pathName)
    {
        CoalesceRecordset eventRecordSet = CoalesceRecordset.create(section, pathName);

        CoalesceFieldDefinition.create(eventRecordSet,
                                       GDELTEventConstants.GlobalEventID,
                                       ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(eventRecordSet, GDELTEventConstants.Day, ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(eventRecordSet, GDELTEventConstants.MonthYear, ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(eventRecordSet, GDELTEventConstants.Year, ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(eventRecordSet, GDELTEventConstants.FractionDate, ECoalesceFieldDataTypes.FLOAT_TYPE);

        CoalesceFieldDefinition.create(eventRecordSet,
                                       GDELTEventConstants.IsRootEvent,
                                       ECoalesceFieldDataTypes.BOOLEAN_TYPE);
        CoalesceFieldDefinition.create(eventRecordSet, GDELTEventConstants.EventCode, ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(eventRecordSet,
                                       GDELTEventConstants.EventBaseCode,
                                       ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(eventRecordSet,
                                       GDELTEventConstants.EventRootCode,
                                       ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(eventRecordSet, GDELTEventConstants.QuadClass, ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(eventRecordSet,
                                       GDELTEventConstants.GoldsteinScale,
                                       ECoalesceFieldDataTypes.FLOAT_TYPE);
        CoalesceFieldDefinition.create(eventRecordSet,
                                       GDELTEventConstants.NumMentions,
                                       ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(eventRecordSet, GDELTEventConstants.NumSources, ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(eventRecordSet,
                                       GDELTEventConstants.NumArticles,
                                       ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(eventRecordSet, GDELTEventConstants.AvgTone, ECoalesceFieldDataTypes.FLOAT_TYPE);

        CoalesceFieldDefinition.create(eventRecordSet,
                                       GDELTEventConstants.ActionGeoType,
                                       ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(eventRecordSet,
                                       GDELTEventConstants.ActionGeoFullname,
                                       ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(eventRecordSet,
                                       GDELTEventConstants.ActionGeoCountryCode,
                                       ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(eventRecordSet,
                                       GDELTEventConstants.ActionGeoADM1Code,
                                       ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(eventRecordSet,
                                       GDELTEventConstants.ActionGeoADM2Code,
                                       ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(eventRecordSet,
                                       GDELTEventConstants.ActionGeoFeatureID,
                                       ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(eventRecordSet, GDELTEventConstants.DateAdded, ECoalesceFieldDataTypes.DATE_TIME_TYPE);
        CoalesceFieldDefinition.create(eventRecordSet, GDELTEventConstants.SourceURL, ECoalesceFieldDataTypes.STRING_TYPE);

        CoalesceFieldDefinition.create(eventRecordSet,
                                       GDELTEventConstants.ActionGeoLocation,
                                       ECoalesceFieldDataTypes.GEOCOORDINATE_TYPE);

        return eventRecordSet;
    }

    public int getGlobalEventID()
    {
        try
        {
            return ((CoalesceIntegerField) this.getFieldByName(GDELTEventConstants.GlobalEventID)).getValue();
        }
        catch (CoalesceDataFormatException e)
        {
            LOGGER.error(e.getMessage(), e);
            return 0;
        }
    }

    public void setGlobalEventID(int globalEventID)
    {
        ((CoalesceIntegerField) this.getFieldByName(GDELTEventConstants.GlobalEventID)).setValue(globalEventID);
    }

    public int getDay()
    {
        try
        {
            return ((CoalesceIntegerField) this.getFieldByName(GDELTEventConstants.Day)).getValue();
        }
        catch (CoalesceDataFormatException e)
        {
            LOGGER.error(e.getMessage(), e);
            return 0;
        }
    }

    public void setDay(int day)
    {
        ((CoalesceIntegerField) this.getFieldByName(GDELTEventConstants.Day)).setValue(day);
    }

    public int getMonthYear()
    {
        try
        {
            return ((CoalesceIntegerField) this.getFieldByName(GDELTEventConstants.MonthYear)).getValue();
        }
        catch (CoalesceDataFormatException e)
        {
            LOGGER.error(e.getMessage(), e);
            return 0;
        }
    }

    public void setMonthYear(int monthYear)
    {
        ((CoalesceIntegerField) this.getFieldByName(GDELTEventConstants.MonthYear)).setValue(monthYear);
    }

    public int getYear()
    {
        try
        {
            return ((CoalesceIntegerField) this.getFieldByName(GDELTEventConstants.Year)).getValue();
        }
        catch (CoalesceDataFormatException e)
        {
            LOGGER.error(e.getMessage(), e);
            return 0;
        }
    }

    public void setYear(int year)
    {
        ((CoalesceIntegerField) this.getFieldByName(GDELTEventConstants.Year)).setValue(year);
    }

    public float getFractionDate()
    {
        try
        {
            return ((CoalesceFloatField) this.getFieldByName(GDELTEventConstants.FractionDate)).getValue();
        }
        catch (CoalesceDataFormatException e)
        {
            LOGGER.error(e.getMessage(), e);
            return 0;
        }
    }

    public void setFractionDate(float fractionDate)
    {
        ((CoalesceFloatField) this.getFieldByName(GDELTEventConstants.FractionDate)).setValue(fractionDate);
    }

    public boolean isIsRootEvent()
    {
        try
        {
            return ((CoalesceBooleanField) this.getFieldByName(GDELTEventConstants.IsRootEvent)).getValue();
        }
        catch (CoalesceDataFormatException e)
        {
            LOGGER.error(e.getMessage(), e);
            return false;
        }
    }

    public void setIsRootEvent(boolean isRootEvent)
    {
        ((CoalesceBooleanField) this.getFieldByName(GDELTEventConstants.IsRootEvent)).setValue(isRootEvent);
    }

    public String getEventCode()
    {
        return ((CoalesceStringField) this.getFieldByName(GDELTEventConstants.EventCode)).getValue();
    }

    public void setEventCode(String eventCode)
    {
        ((CoalesceStringField) this.getFieldByName(GDELTEventConstants.EventCode)).setValue(eventCode);
    }

    public String getEventBaseCode()
    {
        return ((CoalesceStringField) this.getFieldByName(GDELTEventConstants.EventBaseCode)).getValue();
    }

    public void setEventBaseCode(String eventBaseCode)
    {
        ((CoalesceStringField) this.getFieldByName(GDELTEventConstants.EventBaseCode)).setValue(eventBaseCode);
    }

    public String getEventRootCode()
    {
        return ((CoalesceStringField) this.getFieldByName(GDELTEventConstants.EventRootCode)).getValue();
    }

    public void setEventRootCode(String eventRootCode)
    {
        ((CoalesceStringField) this.getFieldByName(GDELTEventConstants.EventRootCode)).setValue(eventRootCode);
    }

    public int getQuadClass()
    {
        try
        {
            return ((CoalesceIntegerField) this.getFieldByName(GDELTEventConstants.QuadClass)).getValue();
        }
        catch (CoalesceDataFormatException e)
        {
            LOGGER.error(e.getMessage(), e);
            return 0;
        }
    }

    public void setQuadClass(int quadClass)
    {
        ((CoalesceIntegerField) this.getFieldByName(GDELTEventConstants.QuadClass)).setValue(quadClass);
    }

    public float getGoldsteinScale()
    {
        try
        {
            return ((CoalesceFloatField) this.getFieldByName(GDELTEventConstants.GoldsteinScale)).getValue();
        }
        catch (CoalesceDataFormatException e)
        {
            LOGGER.error(e.getMessage(), e);
            return Float.NaN;
        }
    }

    public void setGoldsteinScale(float goldsteinScale)
    {
        ((CoalesceFloatField) this.getFieldByName(GDELTEventConstants.GoldsteinScale)).setValue(goldsteinScale);
    }

    public int getNumMentions()
    {
        try
        {
            return ((CoalesceIntegerField) this.getFieldByName(GDELTEventConstants.NumMentions)).getValue();
        }
        catch (CoalesceDataFormatException e)
        {
            LOGGER.error(e.getMessage(), e);
            return 0;
        }
    }

    public void setNumMentions(int numMentions)
    {
        ((CoalesceIntegerField) this.getFieldByName(GDELTEventConstants.NumMentions)).setValue(numMentions);
    }

    public int getNumSources()
    {
        try
        {
            return ((CoalesceIntegerField) this.getFieldByName(GDELTEventConstants.NumSources)).getValue();
        }
        catch (CoalesceDataFormatException e)
        {
            LOGGER.error(e.getMessage(), e);
            return 0;
        }
    }

    public void setNumSources(int numSources)
    {
        ((CoalesceIntegerField) this.getFieldByName(GDELTEventConstants.NumSources)).setValue(numSources);
    }

    public int getNumArticles()
    {
        try
        {
            return ((CoalesceIntegerField) this.getFieldByName(GDELTEventConstants.NumArticles)).getValue();
        }
        catch (CoalesceDataFormatException e)
        {
            LOGGER.error(e.getMessage(), e);
            return 0;
        }
    }

    public void setNumArticles(int numArticles)
    {
        ((CoalesceIntegerField) this.getFieldByName(GDELTEventConstants.NumArticles)).setValue(numArticles);
    }

    public float getAvgTone()
    {
        try
        {
            return ((CoalesceFloatField) this.getFieldByName(GDELTEventConstants.AvgTone)).getValue();
        }
        catch (CoalesceDataFormatException e)
        {
            LOGGER.error(e.getMessage(), e);
            return Float.NaN;
        }
    }

    public void setAvgTone(float avgTone)
    {
        ((CoalesceFloatField) this.getFieldByName(GDELTEventConstants.AvgTone)).setValue(avgTone);
    }

    public int getActionGeoType()
    {
        try
        {
            return ((CoalesceIntegerField) this.getFieldByName(GDELTEventConstants.ActionGeoType)).getValue();
        }
        catch (CoalesceDataFormatException e)
        {
            LOGGER.error(e.getMessage(), e);
            return 0;
        }
    }

    public void setActionGeoType(int actionGeoType)
    {
        ((CoalesceIntegerField) this.getFieldByName(GDELTEventConstants.ActionGeoType)).setValue(actionGeoType);
    }

    public String getActionGeoFullname()
    {
        return ((CoalesceStringField) this.getFieldByName(GDELTEventConstants.ActionGeoFullname)).getValue();
    }

    public void setActionGeoFullname(String actionGeoFullname)
    {
        ((CoalesceStringField) this.getFieldByName(GDELTEventConstants.ActionGeoFullname)).setValue(actionGeoFullname);
    }

    public String getActionGeoCountryCode()
    {
        return ((CoalesceStringField) this.getFieldByName(GDELTEventConstants.ActionGeoCountryCode)).getValue();
    }

    public void setActionGeoCountryCode(String actionGeoCountryCode)
    {
        ((CoalesceStringField) this.getFieldByName(GDELTEventConstants.ActionGeoCountryCode)).setValue(actionGeoCountryCode);
    }

    public String getActionGeoADM1Code()
    {
        return ((CoalesceStringField) this.getFieldByName(GDELTEventConstants.ActionGeoADM1Code)).getValue();
    }

    public void setActionGeoADM1Code(String actionGeoADM1Code)
    {
        ((CoalesceStringField) this.getFieldByName(GDELTEventConstants.ActionGeoADM1Code)).setValue(actionGeoADM1Code);
    }

    public String getActionGeoADM2Code()
    {
        return ((CoalesceStringField) this.getFieldByName(GDELTEventConstants.ActionGeoADM2Code)).getValue();
    }

    public void setActionGeoADM2Code(String actionGeoADM2Code)
    {
        ((CoalesceStringField) this.getFieldByName(GDELTEventConstants.ActionGeoADM2Code)).setValue(actionGeoADM2Code);
    }

    public String getActionGeoFeatureID()
    {
        return ((CoalesceStringField) this.getFieldByName(GDELTEventConstants.ActionGeoFeatureID)).getValue();
    }

    public void setActionGeoFeatureID(String actionGeoFeatureID)
    {
        ((CoalesceStringField) this.getFieldByName(GDELTEventConstants.ActionGeoFeatureID)).setValue(actionGeoFeatureID);
    }

    public String getSourceURL()
    {
        return ((CoalesceStringField) this.getFieldByName(GDELTEventConstants.SourceURL)).getValue();
    }

    public void setSourceURL(String sourceURL)
    {
        ((CoalesceStringField) this.getFieldByName(GDELTEventConstants.SourceURL)).setValue(sourceURL);
    }

    public DateTime getDateAdded()
    {
        try
        {
            return ((CoalesceDateTimeField) this.getFieldByName(GDELTEventConstants.DateAdded)).getValue();
        }
        catch (CoalesceDataFormatException e)
        {
            LOGGER.error(e.getMessage(), e);
            return new DateTime(0);
        }
    }

    public void setDateAdded(DateTime dateAdded)
    {
        ((CoalesceDateTimeField) this.getFieldByName(GDELTEventConstants.DateAdded)).setValue(dateAdded);
    }

    public Coordinate getActionGeoLocation()
    {
        try
        {
            return ((CoalesceCoordinateField) this.getFieldByName(GDELTEventConstants.ActionGeoLocation)).getValue();
        }
        catch (CoalesceDataFormatException e)
        {
            LOGGER.error(e.getMessage(), e);
            return null;
        }
    }

    public void setActionGeoLocation(Coordinate actionGeoLocation)
    {
        try
        {
            ((CoalesceCoordinateField) this.getFieldByName(GDELTEventConstants.ActionGeoLocation)).setValue(actionGeoLocation);
        }
        catch (CoalesceDataFormatException e)
        {
            LOGGER.error(e.getMessage(), e);
        }
    }

}
