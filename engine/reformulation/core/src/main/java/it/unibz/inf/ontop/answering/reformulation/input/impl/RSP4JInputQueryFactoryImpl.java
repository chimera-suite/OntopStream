package it.unibz.inf.ontop.answering.reformulation.input.impl;

import it.polimi.deib.sr.rsp.api.querying.ContinuousQuery;
import it.polimi.deib.sr.rsp.yasper.querying.syntax.QueryFactory;

import com.google.inject.Inject;
import it.unibz.inf.ontop.answering.reformulation.input.*;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.parser.ParsedQuery;

public class RSP4JInputQueryFactoryImpl implements RDF4JInputQueryFactory, RSP4JInputQueryFactory{

    @Inject
    private RSP4JInputQueryFactoryImpl() {
    }

    @Override
    public SelectQuery createSelectQuery(String queryString, ParsedQuery parsedQuery, ContinuousQuery parsedCQ) {
        return new RSP4JSelectQuery(parsedQuery, queryString, parsedCQ);
    }

    @Override
    public SelectQuery createSelectQuery(String queryString, ParsedQuery parsedQuery) {

        ContinuousQuery parsedCQ = QueryFactory.parse(queryString);

        //TODO:IMPLEMENT EXCEPTION
        if (parsedCQ.getWindowMap().size() > 1) {
            throw new MalformedQueryException("Too many windows (Max support is 1)\n" + queryString);
        }

        return new RSP4JSelectQuery(parsedQuery, queryString, parsedCQ);
    }

    @Override
    public AskQuery createAskQuery(String queryString, ParsedQuery parsedQuery) {
        throw new UnsupportedOperationException("createAskQuery not supported");
    }

    @Override
    public ConstructQuery createConstructQuery(String queryString, ParsedQuery parsedQuery) {
        throw new UnsupportedOperationException("createConstructQuery not supported");
    }

    @Override
    public DescribeQuery createDescribeQuery(String queryString, ParsedQuery parsedQuery) {
        throw new UnsupportedOperationException("createDescribeQuery not supported");
    }
}
