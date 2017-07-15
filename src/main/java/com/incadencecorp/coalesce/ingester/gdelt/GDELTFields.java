package com.incadencecorp.coalesce.ingester.gdelt;

public interface GDELTFields {

 
    // Token to split GDELT files on
    String SplitToken = "\t";

    public enum Fields
    {
        GlobalEventId(0),
        Day(1),
        MonthYear(2),
        Year(3),
        FractionDate(4),
        Agent1Code(5),
        Agent1Name(6),
        Agent1CountryCode(7),
        Agent1KnownGroupCode(8),
        Agent1EthnicCode(9),
        Agent1Religion1Code(10),
        Agent1Religion2Code(11),
        Agent1Type1Code(12),
        Agent1Type2Code(13),
        Agent1Type3Code(14),
        Agent2Code(15),
        Agent2Name(16),
        Agent2CountryCode(17),
        Agent2KnownGroupCode(18),
        Agent2EthnicCode(19),
        Agent2Religion1Code(20),
        Agent2Religion2Code(21),
        Agent2Type1Code(22),
        Agent2Type2Code(23),
        Agent2Type3Code(24),
        IsRootEvent(25),
        EventCode(26),
        EventBaseCode(27),
        EventRootCode(28),
        QuadClass(29),
        GoldsteinScale(30),
        NumMentions(31),
        NumSources(32),
        NumArticles(33),
        AvgTone(34),
        Agent1Geo_Type(35),
        Agent1Geo_Fullname(36),
        Agent1Geo_CountryCode(37),
        Agent1Geo_ADM1Code(38),
        Agent1Geo_ADM2Code(39),
        Agent1Geo_Lat(40),
        Agent1Geo_Long(41),
        Agent1Geo_FeatureID(42),
        Agent2Geo_Type(43),
        Agent2Geo_Fullname(44),
        Agent2Geo_CountryCode(45),
        Agent2Geo_ADM1Code(46),
        Agent2Geo_ADM2Code(47),
        Agent2Geo_Lat(48),
        Agent2Geo_Long(49),
        Agent2Geo_FeatureID(50),
        ActionGeo_Type(51),
        ActionGeo_Fullname(52),
        ActionGeo_CountryCode(53),
        ActionGeo_ADM1Code(54),
        ActionGeo_ADM2Code(55),
        ActionGeo_Lat(56),
        ActionGeo_Long(57),
        ActionGeo_FeatureID(58),
        DATEADDED(59),
        SOURCEURL(60);

        public int value;

        private Fields(int value)
        {
            this.value = value;
        }
    }
}
