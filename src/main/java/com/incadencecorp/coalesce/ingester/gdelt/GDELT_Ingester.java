package com.incadencecorp.coalesce.ingester.gdelt;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.codec.language.DoubleMetaphone;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import com.incadencecorp.coalesce.common.exceptions.CoalescePersistorException;
import com.incadencecorp.coalesce.common.helpers.EntityLinkHelper;
import com.incadencecorp.coalesce.framework.CoalesceFramework;
import com.incadencecorp.coalesce.framework.CoalesceObjectFactory;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceDateTimeField;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceEntity;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceEntityTemplate;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecord;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecordset;
import com.incadencecorp.coalesce.framework.datamodel.ELinkTypes;
import com.incadencecorp.coalesce.framework.persistance.ICoalescePersistor;
import com.incadencecorp.coalesce.framework.persistance.ServerConn;
import com.incadencecorp.coalesce.framework.persistance.accumulo.AccumuloPersistor;
import com.incadencecorp.coalesce.framework.persistance.accumulo.AccumuloSettings;

public class GDELT_Ingester {

    private String source = "GDELT";
    private static final Logger LOGGER = LoggerFactory.getLogger(GDELT_Ingester.class);

    /**
     * Method to create Entities from version 2.X GDELT data
     * 
     * @param gdeltLine
     * @return GDELT_Entity created from the ingest
     */
    private List<OEEntity> createV2EntitiesFromLine(String gdeltLine, String sourceFile)
    {

        List<OEEntity> entities = new ArrayList<>();

        GDELTArtifact artifact = new GDELTArtifact();

        artifact.setSource(source);
        CoalesceRecordset gdelt_artifactRecordSet = artifact.getCoalesceRecordsetForNamePath("OEEntity/GDELTArtifactSection/GDELTArtifactRecordset");
        CoalesceRecord gdelt_artifactRecord = gdelt_artifactRecordSet.getItem(0);

        CoalesceRecordset artifactRecordSet = artifact.getCoalesceRecordsetForNamePath("OEEntity/ArtifactSection/ArtifactRecordset");
        CoalesceRecord artifactRecord = artifactRecordSet.getItem(0);

        entities.add(artifact);

        String[] fields = gdeltLine.split("\t");
        if (fields.length == 61)
        {

            // Generate the DateTime before we start
            int year = NumberUtils.toInt(fields[59].substring(0, 4));
            int month = NumberUtils.toInt(fields[59].substring(4, 6));
            int day = NumberUtils.toInt(fields[59].substring(6, 8));
            int hour = NumberUtils.toInt(fields[59].substring(8, 10));
            int min = NumberUtils.toInt(fields[59].substring(10, 12));
            int sec = NumberUtils.toInt(fields[59].substring(12, 14));
            DateTime dt = new DateTime(year, month, day, hour, min, sec);

            // Populate the Artifact with the raw data and metadata
            GDELTArtifact.setStringField(gdelt_artifactRecord, "SourceFileName", sourceFile);
            GDELTArtifact.setIntegerField(gdelt_artifactRecord, "GlobalEventID", fields[0]);
            GDELTArtifact.setStringField(gdelt_artifactRecord, "RawText", StringEscapeUtils.escapeJava(gdeltLine));

            ((CoalesceDateTimeField) artifactRecord.getFieldByName("DateIngested")).setValue(DateTime.now());
            DateTime artifactDate = new DateTime(Integer.parseInt(sourceFile.substring(0, 4)),
                                                 Integer.parseInt(sourceFile.substring(4, 6)),
                                                 Integer.parseInt(sourceFile.substring(6, 8)),
                                                 0,
                                                 0);
            ((CoalesceDateTimeField) artifactRecord.getFieldByName("ArtifactDate")).setValue(artifactDate);

            // Generate an MD5 Sum for the data chunk
            try
            {
                MessageDigest m = MessageDigest.getInstance("MD5");
                m.update(gdeltLine.getBytes());
                byte[] digest = m.digest();
                BigInteger bigInt = new BigInteger(1, digest);
                String hashText = bigInt.toString(16); // We need to zero pad it to get the full 32 chars.
                while (hashText.length() < 32)
                {
                    hashText = "0" + hashText;
                }
                OEEntity.setStringField(artifactRecord, "Md5Sum", hashText);
            }
            catch (NoSuchAlgorithmException e)
            {
                e.printStackTrace();
            }

            // Actor1 section
            OEActor actor1 = new OEActor();

            actor1.setSource(source);
            CoalesceRecordset actor1RecordSet = actor1.getCoalesceRecordsetForNamePath("OEEntity/ActorSection/ActorRecordset");
            CoalesceRecord actor1Record = actor1RecordSet.getItem(0);
            CoalesceRecordset actor1OERecordSet = actor1.getCoalesceRecordsetForNamePath("OEEntity/OESection/OERecordset");
            CoalesceRecord actor1OERecord = actor1OERecordSet.getItem(0);

            OEActor.setBooleanField(actor1OERecord, "IsSimulation", false);
            OEActor.setStringField(actor1OERecord, "DataSource", "GDELT");
            OEActor.setStringField(actor1OERecord, "OntologyReference", "Actor");

            OEActor.setStringField(actor1Record, "ActorCode", fields[5]);
            OEActor.setStringField(actor1Record, "ActorName", fields[6]);
            OEActor.setStringField(actor1Record, "ActorCountryCode", fields[7]);
            OEActor.setStringField(actor1Record, "ActorKnownGroupCode", fields[8]);
            OEActor.setStringField(actor1Record, "ActorEthnicCode", fields[9]);
            OEActor.setStringField(actor1Record, "ActorReligion1Code", fields[10]);
            OEActor.setStringField(actor1Record, "ActorReligion2Code", fields[11]);
            OEActor.setStringField(actor1Record, "ActorType1Code", fields[12]);
            OEActor.setStringField(actor1Record, "ActorType2Code", fields[13]);
            OEActor.setStringField(actor1Record, "ActorType3Code", fields[14]);

            OEActor.setIntegerField(actor1Record, "ActorGeoType", fields[35]);
            OEActor.setStringField(actor1Record, "ActorGeoFullname", fields[36]);
            OEActor.setStringField(actor1Record, "ActorGeoCountryCode", fields[37]);
            OEActor.setStringField(actor1Record, "ActorGeoADM1Code", fields[38]);
            OEActor.setStringField(actor1Record, "ActorGeoADM2Code", fields[39]);
            OEActor.setStringField(actor1Record, "ActorGeoFeatureID", fields[42]);

            if (!fields[40].isEmpty() && !fields[41].isEmpty())
            {
                OEActor.buildAndSetGeoField(actor1Record,
                                            "ActorGeo",
                                            Float.parseFloat(fields[40]),
                                            Float.parseFloat(fields[41]));
            } else {
            	LOGGER.debug("Empty Lat/Long for Actor1");
            }
            entities.add(actor1);

            // Actor2 section
            OEActor actor2 = new OEActor();
            actor2.setSource(source);
            CoalesceRecordset actor2RecordSet = actor2.getCoalesceRecordsetForNamePath("OEEntity/ActorSection/ActorRecordset");
            CoalesceRecord actor2Record = actor2RecordSet.getItem(0);
            CoalesceRecordset actor2OERecordSet = actor2.getCoalesceRecordsetForNamePath("OEEntity/OESection/OERecordset");
            CoalesceRecord actor2OERecord = actor2OERecordSet.getItem(0);

            OEActor.setBooleanField(actor2OERecord, "IsSimulation", false);
            OEActor.setStringField(actor2OERecord, "DataSource", "GDELT");
            OEActor.setStringField(actor1OERecord, "OntologyReference", "Actor");

            OEActor.setStringField(actor2Record, "ActorCode", fields[15]);
            OEActor.setStringField(actor2Record, "ActorName", fields[16]);
            OEActor.setStringField(actor2Record, "NameMetaphone", new DoubleMetaphone().doubleMetaphone(fields[16]));
            OEActor.setStringField(actor2Record, "ActorCountryCode", fields[17]);
            OEActor.setStringField(actor2Record, "ActorKnownGroupCode", fields[18]);
            OEActor.setStringField(actor2Record, "ActorEthnicCode", fields[19]);
            OEActor.setStringField(actor2Record, "ActorReligion1Code", fields[20]);
            OEActor.setStringField(actor2Record, "ActorReligion2Code", fields[21]);
            OEActor.setStringField(actor2Record, "ActorType1Code", fields[22]);
            OEActor.setStringField(actor2Record, "ActorType2Code", fields[23]);
            OEActor.setStringField(actor2Record, "ActorType3Code", fields[24]);

            OEActor.setIntegerField(actor2Record, "ActorGeoType", fields[43]);
            OEActor.setStringField(actor2Record, "ActorGeoFullname", fields[44]);
            OEActor.setStringField(actor2Record, "ActorGeoCountryCode", fields[45]);
            OEActor.setStringField(actor2Record, "ActorGeoADM1Code", fields[46]);
            OEActor.setStringField(actor2Record, "ActorGeoADM2Code", fields[47]);
            OEActor.setStringField(actor2Record, "ActorGeoFeatureID", fields[50]);

            if (!fields[48].isEmpty() && !fields[49].isEmpty())
            {
                OEActor.buildAndSetGeoField(actor2Record,
                                            "ActorGeo",
                                            Float.parseFloat(fields[48]),
                                            Float.parseFloat(fields[49]));
            } else {
            	LOGGER.debug("Empty Lat/Long for Actor2");
            }
            entities.add(actor2);

            // Event section
            OEEvent event = new OEEvent();

            event.setSource(source);
            CoalesceRecordset eventRecordSet = event.getCoalesceRecordsetForNamePath("OEEntity/EventSection/EventRecordset");
            CoalesceRecord eventRecord = eventRecordSet.getItem(0);
            CoalesceRecordset eventOERecordSet = event.getCoalesceRecordsetForNamePath("OEEntity/OESection/OERecordset");
            CoalesceRecord eventOERecord = eventOERecordSet.getItem(0);

            OEActor.setBooleanField(eventOERecord, "IsSimulation", false);
            OEActor.setStringField(eventOERecord, "DataSource", "GDELT");
            OEActor.setStringField(actor1OERecord, "OntologyReference", "Event");

            OEEvent.setIntegerField(eventRecord, "GlobalEventID", fields[0]);
            OEEvent.setIntegerField(eventRecord, "Day", fields[1]);
            OEEvent.setIntegerField(eventRecord, "MonthYear", fields[2]);
            OEEvent.setIntegerField(eventRecord, "Year", fields[3]);
            OEEvent.setFloatField(eventRecord, "FractionDate", fields[4]);

            OEEvent.setIntegerField(eventRecord, "IsRootEvent", fields[25]);
            OEEvent.setStringField(eventRecord, "EventCode", fields[26]);
            OEEvent.setStringField(eventRecord, "EventBaseCode", fields[27]);
            OEEvent.setStringField(eventRecord, "EventRootCode", fields[28]);
            OEEvent.setIntegerField(eventRecord, "QuadClass", fields[29]);
            OEEvent.setFloatField(eventRecord, "GoldsteinScale", fields[30]);
            OEEvent.setIntegerField(eventRecord, "NumMentions", fields[30]);
            OEEvent.setIntegerField(eventRecord, "NumSources", fields[31]);
            OEEvent.setIntegerField(eventRecord, "NumArticles", fields[32]);
            OEEvent.setFloatField(eventRecord, "AvgTone", fields[34]);

            OEEvent.setIntegerField(eventRecord, "ActionGeoType", fields[51]);
            OEEvent.setStringField(eventRecord, "ActionGeoFullname", fields[52]);
            OEEvent.setStringField(eventRecord, "ActionGeoCountryCode", fields[53]);
            OEEvent.setStringField(eventRecord, "ActionGeoADM1Code", fields[54]);
            OEEvent.setStringField(eventRecord, "ActionGeoADM2Code", fields[55]);
            OEEvent.setStringField(eventRecord, "ActionGeoFeatureID", fields[58]);
            OEEvent.setIntegerField(eventRecord, "DateAdded", fields[59]);
            OEEvent.setStringField(eventRecord, "SourceURL", fields[60]);
            ((CoalesceDateTimeField) eventRecord.getFieldByName("DateTime")).setValue(dt);

            if (!fields[56].isEmpty() && !fields[57].isEmpty())
            {
                OEEvent.buildAndSetGeoField(eventRecord,
                                            "ActionGeo",
                                            Float.parseFloat(fields[56]),
                                            Float.parseFloat(fields[57]));
            } else {
            	LOGGER.debug("Empty Lat/Long for Event");
            }

            // Linkage Section
            // TODO: Use the non-deprecated version with all the extra stuff
            //EntityLinkHelper.linkEntities(artifact, ELinkTypes.HAS_PRODUCT, actor1, false);
            //EntityLinkHelper.linkEntities(artifact, ELinkTypes.HAS_PRODUCT, actor2, false);
            //EntityLinkHelper.linkEntities(artifact, ELinkTypes.HAS_PRODUCT, event, false);

            //EntityLinkHelper.linkEntities(event, ELinkTypes.IS_PRODUCT_OF, artifact, false);
            EntityLinkHelper.linkEntities(event, ELinkTypes.HAS_PARTICIPANT, actor1, false);
            EntityLinkHelper.linkEntities(event, ELinkTypes.HAS_PARTICIPANT, actor2, false);

            //EntityLinkHelper.linkEntities(actor1, ELinkTypes.IS_PRODUCT_OF, artifact, false);
            EntityLinkHelper.linkEntities(actor1, ELinkTypes.IS_A_PARTICIPANT_OF, event, false);

            //EntityLinkHelper.linkEntities(actor2, ELinkTypes.IS_PRODUCT_OF, artifact, false);
            EntityLinkHelper.linkEntities(actor2, ELinkTypes.IS_A_PARTICIPANT_OF, event, false);

            // LOGGER.info("adding event entity");
            entities.add(event);

        }
        return entities;
    }

    public List<CoalesceEntity> loadRecordsFromFile(File file) throws IOException
    {
        List<CoalesceEntity> entities = new ArrayList<>();
        try (BufferedReader fr = new BufferedReader(new FileReader(file)))
        {
            String line;
            while ((line = fr.readLine()) != null)
            {
                entities.addAll(createV2EntitiesFromLine(line, file.getName()));
            }
        }
        return entities;
    }

    public int persistRecordsFromFile(ICoalescePersistor persistor, File file)
            throws IOException, SAXException, CoalescePersistorException
    {
        CoalesceFramework coalesceFramework = new CoalesceFramework();
        coalesceFramework.setAuthoritativePersistor(persistor);
        int count = 0;
        boolean firstEntity = true;
        List<CoalesceEntity> entities = new ArrayList<>();
        try (BufferedReader fr = new BufferedReader(new FileReader(file)))
        {
            String line;
            long beginTime = System.currentTimeMillis();
            while ((line = fr.readLine()) != null)
            {
                try
                {
                    List<OEEntity> generatedEntities = createV2EntitiesFromLine(line, file.getName());
                    if (firstEntity && !generatedEntities.isEmpty())
                    {
                        CoalesceFramework framework = new CoalesceFramework();
                        framework.setAuthoritativePersistor(persistor);
                        // TODO Figure out how to do the template registration better - maybe by standard we should do it in
                        // TODO the entity class
                        //GDELTArtifact artifact = new GDELTArtifact();
                        //artifact.setSource(source);
                        //CoalesceRecordset gdelt_artifactRecordSet = artifact.getCoalesceRecordsetForNamePath("OEEntity/GDELTArtifactSection/GDELTArtifactRecordset");
                        //CoalesceRecord gdelt_artifactRecord = gdelt_artifactRecordSet.addNew();
                        //framework.saveCoalesceEntityTemplate(CoalesceEntityTemplate.create(artifact));
                        OEActor actor1 = new OEActor();
                        actor1.setSource(source);
                        CoalesceRecordset actor1RecordSet = actor1.getCoalesceRecordsetForNamePath("OEEntity/ActorSection/ActorRecordset");
                        CoalesceRecord actorRecord = actor1RecordSet.addNew();
                        framework.saveCoalesceEntityTemplate(CoalesceEntityTemplate.create(actor1));
                        OEEvent event = new OEEvent();
                        event.setSource(source);
                        CoalesceRecordset eventRecordSet = event.getCoalesceRecordsetForNamePath("OEEntity/EventSection/EventRecordset");
                        CoalesceRecord eventRecord = eventRecordSet.addNew();
                        framework.saveCoalesceEntityTemplate(CoalesceEntityTemplate.create(event));
                        framework.close();
                        CoalesceObjectFactory.register(OEEntity.class);
                        CoalesceObjectFactory.register(OEActor.class);
                        //CoalesceObjectFactory.register(GDELTArtifact.class);
                        firstEntity = false;
                    }
                    entities.addAll(generatedEntities);
                    count += generatedEntities.size();
                    if (entities.size() > 100)
                    {
                        long beginTime2 = System.currentTimeMillis();

                        long processTime = beginTime2 - beginTime;
                        double processRate = (double) count / (double) processTime * 1000d;

                        persistor.saveEntity(true, entities.toArray(new CoalesceEntity[entities.size()]));
                        entities.clear();
                        long persistTime = System.currentTimeMillis() - beginTime2;
                        double persistRate = (double) count / (double) persistTime * 1000d;

                        LOGGER.info("{}  @ process rate of {} entities per second, persist rate of {} entities per second.", count, Math.round(processRate),Math.round(persistRate));
                        
//                        LOGGER.info("persist rate of {} entities per second.   Current ms: {}",
//                                    persistRate,
//                                    System.currentTimeMillis());

                        long totalTime = processTime + persistTime;

                        double percentProcessTime = (double) processTime / (double) totalTime * 100;

                        double percentPersistTime = (double) persistTime / (double) totalTime * 100;

                        LOGGER.info("processTime: {} %   persistTime: {} %", Math.round(percentProcessTime), Math.round(percentPersistTime));

                    }
                }
                catch (CoalescePersistorException e)
                {
                    e.printStackTrace();
                }
            }
            if (!entities.isEmpty())
            {
                persistor.saveEntity(true, entities.toArray(new CoalesceEntity[entities.size()]));
                count += entities.size();
            }
        }
        coalesceFramework.close();
        return count;
    }

    public static void main(String[] args)
            throws FileNotFoundException, IOException, CoalescePersistorException, SAXException
    {
        if (args.length != 2)
        {

            for (String arg : args)
            {
                System.out.println("arg: " + arg);
            }

            System.out.println("Usage: java -cp $CLASSPATH com.incadencecorp.coalesce.ingester.gdelt.GDELT_Ingester configfile datafile");
        }
        else
        {

            GDELT_Ingester ingester = new GDELT_Ingester();
            Properties props = new Properties();
            props.load(new FileReader(new File(args[0])));
            File inputFile = new File(args[1]);

            String dbName = props.getProperty("database");
            String zookeepers = props.getProperty("zookeepers");
            String user = props.getProperty("userid");
            String password = props.getProperty("password");

            AccumuloSettings.setPersistFieldDefAttr(false);
            AccumuloSettings.setPersistSectionAttr(false);
            AccumuloSettings.setPersistRecordsetAttr(false);
            AccumuloSettings.setPersistRecordAttr(false);

            ServerConn conn = new ServerConn.Builder().db(dbName).serverName(zookeepers).user(user).password(password).build();
            AccumuloPersistor persistor = new AccumuloPersistor(conn);
            int count = ingester.persistRecordsFromFile(persistor, inputFile);
            System.out.println("Persisted " + count + " records");
            persistor.close();
        }
    }
}
