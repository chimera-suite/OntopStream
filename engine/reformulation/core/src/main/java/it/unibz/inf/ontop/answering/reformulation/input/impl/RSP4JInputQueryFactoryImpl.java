package it.unibz.inf.ontop.answering.reformulation.input.impl;


import com.google.inject.Inject;
import it.unibz.inf.ontop.answering.reformulation.input.*;
import org.eclipse.rdf4j.query.parser.ParsedQuery;

public class RSP4JInputQueryFactoryImpl implements RSP4JInputQueryFactory {

    @Inject
    private RSP4JInputQueryFactoryImpl() {
    }

    @Override
    public SelectQuery createSelectQuery(String queryString, ParsedQuery parsedQuery) {
        return new RSP4JSelectQuery(parsedQuery, queryString);
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
