package it.unibz.inf.ontop.answering.reformulation.input.impl;

import com.google.inject.Inject;
import it.unibz.inf.ontop.answering.reformulation.input.*;
import it.unibz.inf.ontop.exception.OntopInvalidInputQueryException;
import it.unibz.inf.ontop.exception.OntopUnsupportedInputQueryException;
import it.unibz.inf.ontop.answering.reformulation.input.SPARQLQueryUtility;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.parser.*;

import it.polimi.deib.sr.rsp.api.querying.ContinuousQuery;
import it.polimi.deib.sr.rsp.yasper.querying.syntax.QueryFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RSPInputQueryFactoryImpl implements InputQueryFactory {

    private final RSP4JInputQueryFactory rsp4jFactory;

    @Inject
    private RSPInputQueryFactoryImpl(RSP4JInputQueryFactory rdf4jFactory) {
        this.rsp4jFactory = rdf4jFactory;
    }

    @Override
    public SelectQuery createSelectQuery(String queryString) throws OntopInvalidInputQueryException {

        ContinuousQuery parse = QueryFactory.parse(queryString);

        //Map<WindowNode, WebStream> iteration
        parse.getWindowMap().forEach((k, v) -> System.out.println((k.iri() + " "+ k.getRange() + " " +k.getStep()+" | "
                + v.uri())));

        queryString = RSPQLtoSPARQL(queryString);

        ParsedQuery parsedQuery = parseQueryString(queryString);

        System.out.println("xxxxxxxxxx-RSP-PARSED-QUERY-xxxxxxxxxx");
        System.out.println("querystring:\n"+queryString);
        System.out.println("-----------------------");
        /*System.out.println("parsedQuery:\n"+parsedQuery);
        System.out.println("-----------------------");
        System.out.println("rdf4jFactory.createSelectQuery(queryString, parsedQuery):\n"+rsp4jFactory.createSelectQuery(queryString, parsedQuery));*/
        System.out.println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");

        if (parsedQuery instanceof ParsedTupleQuery)
            return rsp4jFactory.createSelectQuery(queryString, parsedQuery);
        else
            throw new OntopInvalidInputQueryException("Not a valid SELECT query: " + queryString);
    }

    @Override
    public AskQuery createAskQuery(String queryString) throws OntopInvalidInputQueryException {
        ParsedQuery parsedQuery = parseQueryString(queryString);

        if (parsedQuery instanceof ParsedBooleanQuery)
            return rsp4jFactory.createAskQuery(queryString, parsedQuery);
        else
            throw new OntopInvalidInputQueryException("Not a valid ASK query: " + queryString);
    }

    @Override
    public ConstructQuery createConstructQuery(String queryString) throws OntopInvalidInputQueryException {
        ParsedQuery parsedQuery = parseQueryString(queryString);

        if ((parsedQuery instanceof ParsedGraphQuery) && SPARQLQueryUtility.isConstructQuery(queryString))
            return rsp4jFactory.createConstructQuery(queryString, parsedQuery);
        else
            throw new OntopInvalidInputQueryException("Not a valid CONSTRUCT query: " + queryString);
    }

    @Override
    public DescribeQuery createDescribeQuery(String queryString) throws OntopInvalidInputQueryException {
        ParsedQuery parsedQuery = parseQueryString(queryString);

        if ((parsedQuery instanceof ParsedGraphQuery) && (!SPARQLQueryUtility.isConstructQuery(queryString)))
            return rsp4jFactory.createDescribeQuery(queryString, parsedQuery);
        else
            throw new OntopInvalidInputQueryException("Not a valid DESCRIBE query: " + queryString);
    }

    @Override
    public SPARQLQuery createSPARQLQuery(String queryString)
            throws OntopInvalidInputQueryException, OntopUnsupportedInputQueryException {
        ParsedQuery parsedQuery = parseQueryString(queryString);

        if (parsedQuery instanceof ParsedTupleQuery)
            return rsp4jFactory.createSelectQuery(queryString, parsedQuery);
        else if (parsedQuery instanceof ParsedBooleanQuery)
            return rsp4jFactory.createAskQuery(queryString, parsedQuery);
        else if (parsedQuery instanceof ParsedGraphQuery)
            return SPARQLQueryUtility.isConstructQuery(queryString)
                    ? rsp4jFactory.createConstructQuery(queryString, parsedQuery)
                    : rsp4jFactory.createDescribeQuery(queryString, parsedQuery);
        else
            throw new OntopUnsupportedInputQueryException("Unsupported query: " + queryString);
    }

    @Override
    public GraphSPARQLQuery createGraphQuery(String queryString) throws OntopInvalidInputQueryException,
            OntopUnsupportedInputQueryException {
        ParsedQuery parsedQuery = parseQueryString(queryString);

        if (parsedQuery instanceof ParsedGraphQuery)
            return SPARQLQueryUtility.isConstructQuery(queryString)
                    ? rsp4jFactory.createConstructQuery(queryString, parsedQuery)
                    : rsp4jFactory.createDescribeQuery(queryString, parsedQuery);
        else
            throw new OntopUnsupportedInputQueryException("Unsupported query: " + queryString);
    }

    private static String RSPQLtoSPARQL(String RSPQLquery){

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
                        m.group().replaceFirst(windowHeader, "") + " UNION ");
            }
        }

        //remove the last "UNION" which is wrong
        final String unionError = "UNION(\\s*)}";
        RSPQLquery = RSPQLquery.replaceAll(unionError, "}");

        return RSPQLquery;
    }

    private static ParsedQuery parseQueryString(String queryString) throws OntopInvalidInputQueryException {
        try {
            return QueryParserUtil.parseQuery(QueryLanguage.SPARQL, queryString, null);
        } catch (MalformedQueryException e) {
            throw new OntopInvalidInputQueryException(e);
        }
    }
}
