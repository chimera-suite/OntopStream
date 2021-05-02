package it.unibz.inf.ontop.rdf4j.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RSP4JHelper {

    public static String RSPQLtoSPARQL(String RSPQLquery){

        final Pattern rangeWindow = Pattern.compile("FROM(\\s+)NAMED(\\s+)WINDOW(\\s+).+(\\s+)ON(\\s+).+(\\s+)" +
                "\\[(\\s*)RANGE(\\s+).+(\\s+)STEP(\\s+).+(\\s*)\\]");

        final Pattern fromToWindow = Pattern.compile("FROM(\\s+)NAMED(\\s+)WINDOW(\\s+).+(\\s+)ON(\\s+).+(\\s+)" +
                "\\[(\\s*)FROM(\\s+).+(\\s+)TO(\\s+).+(\\s+)STEP(\\s+).+(\\s*)\\]");

        List<String> windowNames = new ArrayList<String>();

        Matcher m = rangeWindow.matcher(RSPQLquery);
        while(m.find()) {
            RSPQLquery=RSPQLquery.replace(m.group(),"");

            String [] windowData = m.group().split(" ");
            System.out.println("RSP-->SPARQL: CLEANING WINDOW TEXT: "+windowData[3]+" "+windowData[7]+" "+windowData[9].replace("]","")); //Only for debug purposes - TODO:delete
            windowNames.add(windowData[3]);
        }

        m = fromToWindow.matcher(RSPQLquery);
        while(m.find()) {
            RSPQLquery=RSPQLquery.replace(m.group(),"");

            String [] windowData = m.group().split(" ");
            System.out.println("RSP-->SPARQL: CLEANING WINDOW TEXT: "+windowData[3]); //Only for debug purposes - TODO:delete
            windowNames.add(windowData[3]);
        }

        for (String win : windowNames) {
            Pattern balancedCurlyBrackets = Pattern.compile("WINDOW(\\s+)"+win+"(\\s*)\\{((?:[^}{]+)|.)\\}"); //TODO: add nested recursion
            String windowHeader = "WINDOW(\\s+)"+win+"(\\s*)";

            m = balancedCurlyBrackets.matcher(RSPQLquery);
            while(m.find()) {
                RSPQLquery = RSPQLquery.replace(m.group(),
                        m.group().replaceFirst(windowHeader, "") + " ");
            }
        }
        return RSPQLquery;
    }
}
