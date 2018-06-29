/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.language.grammar;

import static org.testng.AssertJUnit.assertEquals;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.yqlplus.language.logical.StatementOperator;
import com.yahoo.yqlplus.language.operator.JsonOperatorDump;
import com.yahoo.yqlplus.language.operator.OperatorNode;
import com.yahoo.yqlplus.language.parser.ProgramCompileException;
import com.yahoo.yqlplus.language.parser.ProgramParser;
import org.antlr.v4.runtime.RecognitionException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

@Test
public class ProgramParserTest {

    @Test
    public void testSimpleParse() throws IOException, RecognitionException {
        ProgramParser parser = new ProgramParser();
        OperatorNode<StatementOperator> program = parser.parse("frontpage.yql", getClass().getResourceAsStream("frontpage.yql"));
        JsonOperatorDump dump = new JsonOperatorDump();
        // TODO: verify results
    }

    /**
     * Ticket 6861024
     *
     * @throws IOException
     * @throws RecognitionException
     */
    @Test
    public void testAnnotationWithNegativeInteger() throws IOException, RecognitionException {
        ProgramParser parser = new ProgramParser();
        OperatorNode<StatementOperator> program = parser.parse("annotationWithNegativeInteger.yql",
                getClass().getResourceAsStream("annotationWithNegativeInteger.yql"));
        JsonOperatorDump dump = new JsonOperatorDump();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode json = mapper.readTree(dump.dump(program));
        Assert.assertEquals(json.get("arguments").get(0).get(0).get("arguments").get(0).get("arguments").get(1).get("annotations").get("hitLimit").asInt(),
                -38);

        parser = new ProgramParser();
        program = parser.parse("annotationWithPositiveInteger.yql",
                getClass().getResourceAsStream("annotationWithPositiveInteger.yql"));
        dump = new JsonOperatorDump();
        mapper = new ObjectMapper();
        json = mapper.readTree(dump.dump(program));
        Assert.assertEquals(json.get("arguments").get(0).get(0).get("arguments").get(0).get("arguments").get(1).get("annotations").get("hitLimit").asInt(),
                38);
    }

    @Test(expectedExceptions = {ProgramCompileException.class}, expectedExceptionsMessageRegExp = "sampleMalformed.yql:L4:29 missing ';' at '<EOF>'")
    public void testSimpleProgramParse() throws IOException, RecognitionException {
        ProgramParser parser = new ProgramParser();
        OperatorNode<StatementOperator> program = parser.parse("sampleMalformed.yql", getClass().getResourceAsStream("sampleMalformed.yql"));
    }

    @Test
    public void testExpensiveQueryParsing() {
        final String yqlQuery = "select * from sources * where "
                + "([{\"ranked\": false}](foo contains \"a\" "
                + "and ([{\"ranked\": true}](bar contains \"b\" "
                + "or ([{\"ranked\": false}](foo contains \"c\" "
                + "and foo contains ([{\"ranked\": true}]\"d\")))))));";
        ProgramParser parser = new ProgramParser();
        OperatorNode<?> ast;
        long start = System.currentTimeMillis();
        try {
            ast = parser.parse("query", yqlQuery);
        } catch (RecognitionException e) {
            throw new IllegalArgumentException(e);
        }
        long elapsed = System.currentTimeMillis() - start;
        Assert.assertNotNull(ast);
        System.out.println("Parsing required " + elapsed + " ms");
    }
        
    @Test
    public void testAlias() throws Exception {
        String programStr = "PROGRAM (@uuid string,@logo_type string=\"default\"); " + "\n" + 
                "SELECT p.id,p.provider_id, p.provider_name,p.provider_alias, p.provider_url, {\"name\" : plogo.name, \"image\" :  plogo.image} logo" + "\n" + 
                "FROM provider({}) AS p " + "\n" +
                "LEFT JOIN provider_logo(@logo_type,{}) " + "\n" +
                "AS plogo ON p.id = plogo.provider_id  " + "\n" + 
                "WHERE p.id=@uuid" + "\n" +
                "OUTPUT AS provider;";
        ProgramParser parser = new ProgramParser();
        parser.parse("query", programStr);
    }
    
    @Test
    public void testDataSourceWithAt() throws Exception {
        String programStr = "PROGRAM (" + "\n" +
                            "@userId string," + "\n" +
                            "@userIdType string," + "\n" +
                            "@pfIds string = \"all\"," + "\n" +
                            "@region string = \"US\"," + "\n" +
                            "@lang string = \"en-US\"," + "\n" +
                            "@fields string = \"\"" + "\n" +
                            ");" + "\n" +

                            "FROM FinanceUtilsUDF IMPORT parseCsv;" + "\n" +
                            "FROM PortfolioUDF IMPORT getUniqueSymbols;" + "\n" +
                            "FROM PortfolioUDF IMPORT getUserRecord;" + "\n" +

                            "CREATE TEMPORARY TABLE portfolios AS" + "\n" +
                            "(" + "\n" +
                            "SELECT * FROM finance.portfolio(@userId, @userIdType, parseCsv(@pfIds))" + "\n" +
                            ");" + "\n" +

                            "CREATE TEMPORARY TABLE positions AS" + "\n" +
                            "(" + "\n" +
                            "SELECT * FROM finance.portfolio.position(@userId, @userIdType)" + "\n" +
                            ");" + "\n" +

                            "CREATE TEMPORARY TABLE lots AS" + "\n" +
                            "(" + "\n" +
                            "SELECT * FROM finance.portfolio.lot(@userId, @userIdType)" + "\n" +
                            ");" + "\n" +

                            "CREATE TEMPORARY TABLE quotes AS" + "\n" +
                            "(" + "\n" +
                            "SELECT * FROM finance.quote(getUniqueSymbols(@portfolios), parseCsv(@fields), @region, @lang)" + "\n" +
                            ");" + "\n" +

                            "select * from @portfolios | getUserRecord(@positions, @lots, @quotes, @userId, @userIdType)" + "\n" +
                            "output as finance;";
        ProgramParser parser = new ProgramParser();
        parser.parse("query", programStr);
    }

    @Test
    public void testSourceStatementforConvertSource() throws Exception {
      String programStr = 
          "SELECT teamCode, pos, player FROM ( \n" +
              "SELECT    gs.team_id AS teamCode,\n" +
                 "DailyUDF.getFieldWithDefaultValue(sPlayer.first_name, \"N/A\") AS pos, \n" +
                        "{ \n" +
                           " \"id\": DailyUDF.concatDelim([sPlayer.id, gs.depth]),\n" +
                           " \"firstName\": sPlayer.first_name, \n"+
                           " \"lastName\": sPlayer.last_name, \n" +
                           " \"iconURL\": ps.imageUrl \n" +
                        "} AS player \n" +
              "FROM      sports_test.sports.depth_charts gs \n" +
              "LEFT JOIN TempPlayerDetails ps ON ps.player_id = gs.player_id \n" +
              "LEFT JOIN sports_test.sports.players sPlayer ON sPlayer.id = gs.player_id \n" +
              "WHERE     gs.team_id IN (SELECT teamCode FROM TeamCodes) \n" +
          ");";
      assertEquals("(PROGRAM L0:0 [(EXECUTE L0:1 (PROJECT (PROJECT {alias=source} (FILTER (LEFT_JOIN L10:12 (LEFT_JOIN L10:11 (SCAN L10:10 {alias=gs} [sports_test, sports, depth_charts], []), (SCAN L10:11 {alias=ps} [TempPlayerDetails], []), (EQ (READ_FIELD L34:11 ps, player_id), (READ_FIELD L49:11 gs, player_id))), (SCAN L10:12 {alias=sPlayer} [sports_test, sports, players], []), (EQ (READ_FIELD L48:12 sPlayer, id), (READ_FIELD L61:12 gs, player_id))), (IN_QUERY L10:13 (READ_FIELD L10:13 gs, team_id), (PROJECT (SCAN L46:13 {alias=TeamCodes} [TeamCodes], []), [(FIELD (READ_FIELD L32:13 TeamCodes, teamCode), teamCode)]))), [(FIELD (READ_FIELD L10:2 gs, team_id), teamCode), (FIELD (CALL L0:3 [DailyUDF, getFieldWithDefaultValue], [(READ_FIELD L34:3 sPlayer, first_name), (LITERAL L54:3 N/A)]), pos), (FIELD (MAP L0:4 [id, firstName, lastName, iconURL], [(CALL L7:5 [DailyUDF, concatDelim], [(ARRAY L29:5 [(READ_FIELD L29:5 sPlayer, id), (READ_FIELD L41:5 gs, depth)])]), (READ_FIELD L14:6 sPlayer, first_name), (READ_FIELD L13:7 sPlayer, last_name), (READ_FIELD L12:8 ps, imageUrl)]), player)]), [(FIELD (READ_FIELD L7:1 source, teamCode), teamCode), (FIELD (READ_FIELD L17:1 source, pos), pos), (FIELD (READ_FIELD L22:1 source, player), player)]), result1), (OUTPUT L0:1 result1)])",
          new ProgramParser().parse("query", programStr).toString());
    }
    
    @Test
    public void testUnary() throws Exception {
        String programStr = "SELECT * FROM SOURCES * WHERE article CONTAINS ([{\"a\": [\"b\"]}]\"a\");";
        assertEquals(
                "(PROGRAM L0:0 [(EXECUTE L0:1 (FILTER (ALL L22:1 {alias=row}), (CONTAINS (READ_FIELD L30:1 row, article), (LITERAL L62:1 {a=[b]} a))), result1), (OUTPUT L0:1 result1)])",
                new ProgramParser().parse("query", programStr).toString());
    }
    
    @Test
    public void testUnaryNot() throws Exception {
        String programStr = "SELECT foo FROM bar WHERE title CONTAINS \"song\" AND !(title CONTAINS \"lyrics\");";
        assertEquals(
               "(PROGRAM L0:0 [(EXECUTE L0:1 (PROJECT (FILTER (SCAN L16:1 {alias=bar} [bar], []), (AND [(CONTAINS (READ_FIELD L26:1 bar, title), (LITERAL L41:1 song)), (NOT (CONTAINS (READ_FIELD L54:1 bar, title), (LITERAL L69:1 lyrics)))])), [(FIELD (READ_FIELD L7:1 bar, foo), foo)]), result1), (OUTPUT L0:1 result1)])",
                new ProgramParser().parse("query", programStr).toString());
    }
    
    @Test
    public void testTempOutputName() throws Exception {
        String programStr =  "SELECT {'woeid' : ugl.woeid, \n" +
                             "        'city': ugl.display_name, \n" +
                             "               'state' : ugl.country_name, \n" +
                             "               'country' : ugl.country_code \n" +
                             "             } as location, \n" +
                             "             ugl.timezone_name_abbreviation as timezone, \n" +
                             "             ugl.woeid as woeid, \n" +
                             "   {'unit' : @unit, \n" +
                             "   'now': co.temperature \n" +
                             "       }  as temp      \n" +      
                             "FROM  tmp_unified_geo_locations as ugl \n" +
                             "JOIN  tmp_current_observations as co \n" +
                             "ON ugl.record_key = co.record_key \n" +
                             "OUTPUT AS weather_BETTER_OUTPUT;";
        ProgramParser parser = new ProgramParser();
        parser.parse("query", programStr);
        programStr =  "select * from temp1 output as temp;";
        parser.parse("query", programStr);
        programStr = "create temp table allMergedClusters as (select allMergedClusters from protos('1')); \n" +
                     " select * from allMergedClusters output as protos;";
        assertEquals("(PROGRAM L0:0 [(EXECUTE L18:1 (PROJECT (SCAN L70:1 {alias=protos} [protos], [(LITERAL L77:1 1)]), [(FIELD (READ_FIELD L47:1 protos, allMergedClusters), allMergedClusters)]), allMergedClusters), (EXECUTE L1:2 (EVALUATE L15:2 {alias=allMergedClusters} (VARREF L15:2 allMergedClusters)), protos), (OUTPUT L1:2 protos)])",
                parser.parse("query", programStr).toString());
        programStr = "create temporary table allMergedClusters as (select allMergedClusters from protos('1')); \n" +
                     " select * from allMergedClusters output as protos;";
        assertEquals("(PROGRAM L0:0 [(EXECUTE L23:1 (PROJECT (SCAN L75:1 {alias=protos} [protos], [(LITERAL L82:1 1)]), [(FIELD (READ_FIELD L52:1 protos, allMergedClusters), allMergedClusters)]), allMergedClusters), (EXECUTE L1:2 (EVALUATE L15:2 {alias=allMergedClusters} (VARREF L15:2 allMergedClusters)), protos), (OUTPUT L1:2 protos)])",
                parser.parse("query", programStr).toString());
        programStr = "create temp  table allMergedClusters as (select allMergedClusters from protos('1')); \n" +
                     " select * from allMergedClusters output as protos;";
        assertEquals("(PROGRAM L0:0 [(EXECUTE L19:1 (PROJECT (SCAN L71:1 {alias=protos} [protos], [(LITERAL L78:1 1)]), [(FIELD (READ_FIELD L48:1 protos, allMergedClusters), allMergedClusters)]), allMergedClusters), (EXECUTE L1:2 (EVALUATE L15:2 {alias=allMergedClusters} (VARREF L15:2 allMergedClusters)), protos), (OUTPUT L1:2 protos)])",
                parser.parse("query", programStr).toString());
        programStr = "create temporary    table allMergedClusters as (select allMergedClusters from protos('1')); \n" +
                     " select * from allMergedClusters output as protos;";
        assertEquals("(PROGRAM L0:0 [(EXECUTE L26:1 (PROJECT (SCAN L78:1 {alias=protos} [protos], [(LITERAL L85:1 1)]), [(FIELD (READ_FIELD L55:1 protos, allMergedClusters), allMergedClusters)]), allMergedClusters), (EXECUTE L1:2 (EVALUATE L15:2 {alias=allMergedClusters} (VARREF L15:2 allMergedClusters)), protos), (OUTPUT L1:2 protos)])",
                parser.parse("query", programStr).toString());                
        programStr = "create temp \n table allMergedClusters as (select allMergedClusters from protos('1')); \n" +
                     " select * from allMergedClusters output as protos;";
        assertEquals("(PROGRAM L0:0 [(EXECUTE L7:2 (PROJECT (SCAN L59:2 {alias=protos} [protos], [(LITERAL L66:2 1)]), [(FIELD (READ_FIELD L36:2 protos, allMergedClusters), allMergedClusters)]), allMergedClusters), (EXECUTE L1:3 (EVALUATE L15:3 {alias=allMergedClusters} (VARREF L15:3 allMergedClusters)), protos), (OUTPUT L1:3 protos)])",
                parser.parse("query", programStr).toString());
        programStr = "create \n temporary table allMergedClusters as (select allMergedClusters from protos('1')); \n" +
                     " select * from allMergedClusters output as protos;";
        assertEquals("(PROGRAM L0:0 [(EXECUTE L17:2 (PROJECT (SCAN L69:2 {alias=protos} [protos], [(LITERAL L76:2 1)]), [(FIELD (READ_FIELD L46:2 protos, allMergedClusters), allMergedClusters)]), allMergedClusters), (EXECUTE L1:3 (EVALUATE L15:3 {alias=allMergedClusters} (VARREF L15:3 allMergedClusters)), protos), (OUTPUT L1:3 protos)])",
                parser.parse("query", programStr).toString());
        programStr = "create \n temp table allMergedClusters as (select allMergedClusters from protos('1')); \n" +
                " select * from allMergedClusters output as protos;";
        assertEquals("(PROGRAM L0:0 [(EXECUTE L12:2 (PROJECT (SCAN L64:2 {alias=protos} [protos], [(LITERAL L71:2 1)]), [(FIELD (READ_FIELD L41:2 protos, allMergedClusters), allMergedClusters)]), allMergedClusters), (EXECUTE L1:3 (EVALUATE L15:3 {alias=allMergedClusters} (VARREF L15:3 allMergedClusters)), protos), (OUTPUT L1:3 protos)])",
                parser.parse("query", programStr).toString());
    }
}

