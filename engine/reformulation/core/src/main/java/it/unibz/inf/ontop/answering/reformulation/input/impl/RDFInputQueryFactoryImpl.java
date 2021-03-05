package it.unibz.inf.ontop.answering.reformulation.input.impl;

import com.google.inject.Inject;
import it.unibz.inf.ontop.answering.reformulation.input.*;
import it.unibz.inf.ontop.exception.OntopInvalidInputQueryException;
import it.unibz.inf.ontop.exception.OntopUnsupportedInputQueryException;
import it.unibz.inf.ontop.answering.reformulation.input.SPARQLQueryUtility;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.parser.*;



public class RDFInputQueryFactoryImpl implements InputQueryFactory {

    private final RDF4JInputQueryFactory rdf4jFactory;

    @Inject
    private RDFInputQueryFactoryImpl(RDF4JInputQueryFactory rdf4jFactory) {
        this.rdf4jFactory = rdf4jFactory;
    }

    @Override
    public SelectQuery createSelectQuery(String queryString) throws OntopInvalidInputQueryException {
        ParsedQuery parsedQuery = parseQueryString(queryString);

        System.out.println("YYYYYYYYYYY-PARSED-QUERY-YYYYYYYYYYYYYY");
        System.out.println("querystring:\n"+queryString);
        System.out.println("-----------------------");
        System.out.println("parsedQuery:\n"+parsedQuery);
        System.out.println("-----------------------");
        System.out.println("rdf4jFactory.createSelectQuery(queryString, parsedQuery):\n"+rdf4jFactory.createSelectQuery(queryString, parsedQuery));
        System.out.println("YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY");

        if (parsedQuery instanceof ParsedTupleQuery)
            return rdf4jFactory.createSelectQuery(queryString, parsedQuery);
        else
            throw new OntopInvalidInputQueryException("Not a valid SELECT query: " + queryString);
    }

    @Override
    public AskQuery createAskQuery(String queryString) throws OntopInvalidInputQueryException {
        ParsedQuery parsedQuery = parseQueryString(queryString);

        if (parsedQuery instanceof ParsedBooleanQuery)
            return rdf4jFactory.createAskQuery(queryString, parsedQuery);
        else
            throw new OntopInvalidInputQueryException("Not a valid ASK query: " + queryString);
    }

    @Override
    public ConstructQuery createConstructQuery(String queryString) throws OntopInvalidInputQueryException {
        ParsedQuery parsedQuery = parseQueryString(queryString);

        if ((parsedQuery instanceof ParsedGraphQuery) && SPARQLQueryUtility.isConstructQuery(queryString))
            return rdf4jFactory.createConstructQuery(queryString, parsedQuery);
        else
            throw new OntopInvalidInputQueryException("Not a valid CONSTRUCT query: " + queryString);
    }

    @Override
    public DescribeQuery createDescribeQuery(String queryString) throws OntopInvalidInputQueryException {
        ParsedQuery parsedQuery = parseQueryString(queryString);

        if ((parsedQuery instanceof ParsedGraphQuery) && (!SPARQLQueryUtility.isConstructQuery(queryString)))
            return rdf4jFactory.createDescribeQuery(queryString, parsedQuery);
        else
            throw new OntopInvalidInputQueryException("Not a valid DESCRIBE query: " + queryString);
    }

    @Override
    public SPARQLQuery createSPARQLQuery(String queryString)
            throws OntopInvalidInputQueryException, OntopUnsupportedInputQueryException {
        ParsedQuery parsedQuery = parseQueryString(queryString);

        if (parsedQuery instanceof ParsedTupleQuery)
            return rdf4jFactory.createSelectQuery(queryString, parsedQuery);
        else if (parsedQuery instanceof ParsedBooleanQuery)
            return rdf4jFactory.createAskQuery(queryString, parsedQuery);
        else if (parsedQuery instanceof ParsedGraphQuery)
            return SPARQLQueryUtility.isConstructQuery(queryString)
                    ? rdf4jFactory.createConstructQuery(queryString, parsedQuery)
                    : rdf4jFactory.createDescribeQuery(queryString, parsedQuery);
        else
            throw new OntopUnsupportedInputQueryException("Unsupported query: " + queryString);
    }

    @Override
    public GraphSPARQLQuery createGraphQuery(String queryString) throws OntopInvalidInputQueryException,
            OntopUnsupportedInputQueryException {
        ParsedQuery parsedQuery = parseQueryString(queryString);

        if (parsedQuery instanceof ParsedGraphQuery)
            return SPARQLQueryUtility.isConstructQuery(queryString)
                    ? rdf4jFactory.createConstructQuery(queryString, parsedQuery)
                    : rdf4jFactory.createDescribeQuery(queryString, parsedQuery);
        else
            throw new OntopUnsupportedInputQueryException("Unsupported query: " + queryString);
    }

    private static ParsedQuery parseQueryString(String queryString) throws OntopInvalidInputQueryException {
        try {
            return QueryParserUtil.parseQuery(QueryLanguage.SPARQL, queryString, null);
        } catch (MalformedQueryException e) {
            throw new OntopInvalidInputQueryException(e);
        }
    }
}
