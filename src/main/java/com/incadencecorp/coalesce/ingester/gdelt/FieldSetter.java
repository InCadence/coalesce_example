package com.incadencecorp.coalesce.ingester.gdelt;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;

import com.incadencecorp.coalesce.common.exceptions.CoalesceDataFormatException;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceBooleanField;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceCoordinateField;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceCoordinateListField;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceDateTimeField;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceEnumerationField;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceFloatField;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceIntegerField;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceListField;
import com.incadencecorp.coalesce.framework.datamodel.CoalescePolygonField;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecord;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceStringField;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceStringListField;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

import jj2000.j2k.util.ArrayUtil;

import org.joda.time.DateTime;

/**
 * Common static class for setting entity fields of different types
 * 
 * @author ckrentz
 *
 */
public class FieldSetter {
    private static final Logger LOGGER = LoggerFactory.getLogger(FieldSetter.class);

    private static GeometryFactory factory = new GeometryFactory();

    private FieldSetter()
    {

    }

    public static void setIntegerField(CoalesceRecord record, String name, String value)
    {
        if (StringUtils.isNotBlank(value) && NumberUtils.isNumber(value))
        {
            ((CoalesceIntegerField) record.getFieldByName(name)).setValue(NumberUtils.toInt(value));
        }
    }

    public static void setIntegerField(CoalesceRecord eventRecord, String name, int value)
    {
       
            ((CoalesceIntegerField) eventRecord.getFieldByName(name)).setValue(value);
        
    }
    
    public static void setStringField(CoalesceRecord eventRecord, String name, String value)
    {
        if (StringUtils.isNotBlank(value))
        {
            ((CoalesceStringField) eventRecord.getFieldByName(name)).setValue(value);
        }
    }
   
    
    public static void setFloatField(CoalesceRecord eventRecord, String name, String value)
    {
        if (StringUtils.isNotBlank(value))
        {
            ((CoalesceFloatField) eventRecord.getFieldByName(name)).setValue(NumberUtils.toFloat(value));
        }
    }

    public static void setFloatField(CoalesceRecord eventRecord, String name, float value)
    {
        
            ((CoalesceFloatField) eventRecord.getFieldByName(name)).setValue(value);
        
    }
    
    public static void setStringListField(CoalesceRecord record, String name, String[] values)
    {
        if (values.length > 0)
        {
            ((CoalesceStringListField) record.getFieldByName(name)).setValue(values);
        }
    }

    public static void setEnumField(CoalesceRecord eventRecord, String name, Enum value)
    {
        ((CoalesceEnumerationField) eventRecord.getFieldByName(name)).setValue(value.ordinal());
    }

    public static void setBooleanField(CoalesceRecord eventRecord, String name, Boolean value)
    {
        ((CoalesceBooleanField) eventRecord.getFieldByName(name)).setValue(value);
    }

    public static void setDateTimeField(CoalesceRecord eventRecord, String name, DateTime value)
    {
        ((CoalesceDateTimeField) eventRecord.getFieldByName(name)).setValue(value);
    }

    /**
     * Class that generates a geo field based on a lat and long. Takes in a prefix string and appends "Location" to it, so
     * make sure you have a field named <prefix>Location in your record.
     * 
     * @param eventRecord The record that will store the geo field
     * @param prefix The name of the field to store it in. This method will append "Location" to it before saving
     * @param lat The Latitude geo value
     * @param lon The Longitude geo value
     * 
     * @deprecated use {@link #setPointField(CoalesceRecord eventRecord, String field, Float lat, Float lon)} instead.
     */
    @Deprecated
    public static void buildAndSetGeoField(CoalesceRecord eventRecord, String prefix, Float lat, Float lon)
    {
        try
        {

            if ((lat != null) && (lon != null))
            {
            	if (Math.abs(lat) >90 || Math.abs(lon) > 180) {
            		LOGGER.warn("Coordinates out of range - will normalize: LAT: {} LONG: {}",lat,lon);
            	}
            	if (lat < -90.0) lat = (float) -90.0;
            	if (lat > 90) lat = (float) 90.0;
            	if (lon < -180.0) lon = (float) -180.0;
            	if (lon > 180) lon = (float) 180.0;
            	Point point = factory.createPoint(new Coordinate(lon, lat, 0));
                ((CoalesceCoordinateField) eventRecord.getFieldByName(prefix + "Location")).setValue(point);
            } else
            	LOGGER.error("Coordinates null: Lat: {} Lon: {}",(lat == null),(lon == null));
        }
        catch (CoalesceDataFormatException e)
        {
        	LOGGER.error(e.getMessage(),e);
        }
    }
    
    /**
     * Class that generates a point field based on a lat and long. 
     * Field name is used as is from the record.
     * 
     * @param eventRecord The record that will store the geo field
     * @param name The name of the field to store it in. 
     * @param lat The Latitude geo value
     * @param lon The Longitude geo value
     */
    public static void setPointField(CoalesceRecord eventRecord, String field, Float lat, Float lon)
    {
        try
        {

            if ((lat != null) && (lon != null))
            {
            	if (Math.abs(lat) >90 || Math.abs(lon) > 180) {
            		LOGGER.warn("Coordinates out of range - will normalize: LAT: {} LONG: {}",lat,lon);
            	}
            	if (lat < -90.0) lat = (float) -90.0;
            	if (lat > 90) lat = (float) 90.0;
            	if (lon < -180.0) lon = (float) -180.0;
            	if (lon > 180) lon = (float) 180.0;
            	Point point = factory.createPoint(new Coordinate(lon, lat, 0));
                ((CoalesceCoordinateField) eventRecord.getFieldByName(field)).setValue(point);
            } else
            	LOGGER.error("Coordinates null: Lat: {} Lon: {}",(lat == null),(lon == null));
        }
        catch (CoalesceDataFormatException e)
        {
            LOGGER.error(e.getMessage(),e);

        }
    }


/**
 * Class that generates a multi-point field based on an array of lats, and lons. 
 * Field name is used as is from the record.
 * 
 * @param eventRecord The record that will store the geo field
 * @param name The name of the field to store it in. 
 * @param lat The Latitude geo value
 * @param lon The Longitude geo value
 */
public static void setMultiPointField(CoalesceRecord eventRecord, String field, float[] lat, float[] lon)
{
    if (lat.length != lon.length) {
		LOGGER.error("Lat and Lon arrays of different length Lat: {} Lon: {}",lat.length,lon.length);
		return;
	}
	if ((lat == null) || (lon == null)) {
		LOGGER.error("Coordinates null: Lat: {} Lon: {}",(lat == null),(lon == null));
		return;
	}
	Coordinate[] coords = new Coordinate[lat.length];
	for (int i=0;i<lat.length;i++)
	{
		if (Math.abs(lat[i]) >90 || Math.abs(lon[i]) > 180) {
			LOGGER.warn("Coordinates out of range - will normalize: LAT: {} LONG: {}",lat[i],lon[i]);
		}
		float tlat = lat[i],  tlon=lon[i];
		if (tlat < -90.0) tlat = (float) -90.0;
		if (tlat > 90) tlat = (float) 90.0;
		if (tlon < -180.0) tlat = (float) -180.0;
		if (tlon > 180) tlon = (float) 180.0;
		
		coords[i] = new Coordinate(tlon, tlat, 0);

	}
	Arrays.sort(coords);

	setMultiPointField( eventRecord,  field, coords);
}

/**
 * Class that appends additional coordinates to a multi-point field based on an array of lats, and lons. 
 * Field name is used as is from the record.
 * 
 * @param eventRecord The record that will store the geo field
 * @param name The name of the field to store it in. 
 * @param lat The Latitude geo value
 * @param lon The Longitude geo value
 * @throws CoalesceDataFormatException 
 */
public static void appendMultiPointField(CoalesceRecord record, String field, float[] lat, float[] lon) throws CoalesceDataFormatException
{
    if (lat.length != lon.length) {
		LOGGER.error("Lat and Lon arrays of different length Lat: {} Lon: {}",lat.length,lon.length);
		return;
	}
	if ((lat == null) || (lon == null)) {
		LOGGER.error("Coordinates null: Lat: {} Lon: {}",(lat == null),(lon == null));
		return;
	}
	Coordinate[] coords = new Coordinate[lat.length];
	for (int i=0;i<lat.length;i++)
	{
		if (Math.abs(lat[i]) >90 || Math.abs(lon[i]) > 180) {
			LOGGER.warn("Coordinates out of range - will normalize: LAT: {} LONG: {}",lat[i],lon[i]);
		}
		float tlat = lat[i],  tlon=lon[i];
		if (tlat < -90.0) tlat = (float) -90.0;
		if (tlat > 90) tlat = (float) 90.0;
		if (tlon < -180.0) tlat = (float) -180.0;
		if (tlon > 180) tlon = (float) 180.0;
		
		coords[i] = new Coordinate(tlon, tlat, 0);

	}				        
	CoalesceCoordinateListField geoField = (CoalesceCoordinateListField) record.getFieldByName(field);
	ArrayList<Coordinate> currentcoords = new ArrayList<Coordinate>(Arrays.asList(geoField.getValue()));
	for(int i = 0; i < coords.length; i++) {
        if(!currentcoords.contains(coords[i]))
            currentcoords.add(coords[i]);
    }
	Coordinate[] newcoords = currentcoords.toArray(new Coordinate[currentcoords.size()]);
	Arrays.sort(newcoords);
	setMultiPointField( record,  field, newcoords);
}

/**
 * Class that generates a multi-point field based on an array of coordinates. 
 * Field name is used as is from the record.
 * 
 * @param eventRecord The record that will store the geo field
 * @param name The name of the field to store it in. 
 * @param points Coordinate array
 */
public static void setMultiPointField(CoalesceRecord record, String field, Coordinate[] points)
{
    try
    {


            ((CoalesceCoordinateListField) record.getFieldByName(field)).setValue(points);

    }
    catch (CoalesceDataFormatException e)
    {
        LOGGER.error(e.getMessage(),e);
    }
}

/**
 * Class that generates a polygon field based on a bounding box. 
 * Field name is used as is from the record.
 * 
 * @param record The record that will store the geo field
 * @param name The name of the field to store it in. 
 * @param west The Longitude western most value value
 * @param north The Latitude northern most value
 * @param east The Longitude eastern most value
 * @param south The Latitude southern most value
 */
public static void setPolygonField(CoalesceRecord record, String field, float west, float north, float east, float south)
{

	
	Coordinate[] coords = new Coordinate[5];
	
	if (Math.abs(north) >90 || Math.abs(west) > 180 || Math.abs(south) > 90 || Math.abs(east) > 180) {
			LOGGER.warn("Coordinates out of range - will normalize: north: {} west: {}  east: {}  south: {}",north, west, east, south);
		}
	float twest = west,  tnorth = north, teast = east, tsouth = south;
	if (tnorth < -90.0) tnorth = (float) -90.0;
	if (tnorth > 90) tnorth = (float) 90.0;
	if (tsouth < -90.0) tsouth = (float) -90.0;
	if (tsouth > 90) tsouth = (float) 90.0;
	if (twest < -180.0) twest = (float) -180.0;
	if (twest > 180) twest = (float) 180.0;
	if (teast < -180.0) teast = (float) -180.0;
	if (teast > 180) teast = (float) 180.0;	
	
	coords[0] = new Coordinate(twest, tnorth, 0);
	coords[1] = new Coordinate(teast, tnorth, 0);
	coords[2] = new Coordinate(teast, tsouth, 0);
	coords[3] = new Coordinate(twest, tsouth, 0);
	coords[4] = new Coordinate(twest, tnorth, 0);
	
	
	setPolygonField( record,  field,  coords);
	

}

/**
 * Class that generates a polygon field based on an array of lats, and lons. 
 * Field name is used as is from the record.
 * 
 * @param record The record that will store the geo field
 * @param name The name of the field to store it in. 
 * @param coords The array of coordinate values
 */
public static void setPolygonField(CoalesceRecord record, String field, Coordinate[] coords)
{

	// Simply pass an array of Coordinate or a CoordinateSequence to its method
	Polygon poly = factory.createPolygon(coords);
    try
    {


            ((CoalescePolygonField) record.getFieldByName(field)).setValue(poly);

    }
    catch (CoalesceDataFormatException e)
    {
        LOGGER.error(e.getMessage(),e);
    }
}

}