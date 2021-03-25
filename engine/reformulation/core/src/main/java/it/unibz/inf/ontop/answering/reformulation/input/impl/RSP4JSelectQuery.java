package it.unibz.inf.ontop.answering.reformulation.input.impl;

import it.polimi.deib.sr.rsp.api.querying.ContinuousQuery;
import it.unibz.inf.ontop.answering.reformulation.input.SelectQuery;
import it.unibz.inf.ontop.answering.resultset.TupleResultSet;
import org.eclipse.rdf4j.query.parser.ParsedQuery;

public class RSP4JSelectQuery extends RSP4JInputQuery<TupleResultSet> implements SelectQuery {

    /**
     * TODO: support bindings
     */
    RSP4JSelectQuery(ParsedQuery parsedQuery, String queryString, ContinuousQuery parsedCQ) {
        super(parsedQuery, queryString, parsedCQ);
    }
}
