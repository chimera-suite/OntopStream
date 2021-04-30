package it.polimi.deib.rsp.test.examples;

import it.polimi.deib.sr.rsp.yasper.querying.QueryFactory;
import it.polimi.deib.sr.rsp.api.querying.ContinuousQuery;

import java.io.IOException;

public class QueryTest {

    public static void main(String[] args) throws IOException {

        String q = "PREFIX : <http://example.org#>\n" +
                "REGISTER RSTREAM <output> AS\n" +
                "SELECT ?v\n" +
                "FROM NAMED WINDOW <w1> ON :stream1 [RANGE PT5S STEP PT2S]\n" +
                "WHERE {\n" +
                "WINDOW <w1> { ?sensor :value ?v ; :measurement: ?m }\n" +
                "FILTER (?m = 'temperature')\n" +
                "}";



        ContinuousQuery parse = QueryFactory.parse(q);

        System.out.println(parse.toString());

    }
}
