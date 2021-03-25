package it.unibz.inf.ontop.answering.reformulation.input.impl;

import it.unibz.inf.ontop.answering.reformulation.input.InputQuery;
import it.unibz.inf.ontop.answering.reformulation.input.translation.InputQueryTranslator;
import it.polimi.deib.sr.rsp.api.querying.ContinuousQuery;
import it.unibz.inf.ontop.answering.reformulation.input.translation.RSP4JInputQueryTranslator;
import it.unibz.inf.ontop.answering.resultset.OBDAResultSet;
import it.unibz.inf.ontop.exception.OntopInvalidInputQueryException;
import it.unibz.inf.ontop.exception.OntopUnsupportedInputQueryException;
import it.unibz.inf.ontop.iq.IQ;
import org.eclipse.rdf4j.query.parser.ParsedQuery;

public class RSP4JInputQuery<R extends OBDAResultSet> implements InputQuery<R> {

    protected final ParsedQuery parsedQuery;
    private final String inputQueryString;
    private final ContinuousQuery parsedCQ;

    RSP4JInputQuery(ParsedQuery parsedQuery, String inputQueryString, ContinuousQuery parsedCQ) {
        this.parsedQuery = parsedQuery;
        this.inputQueryString = inputQueryString;
        this.parsedCQ = parsedCQ;
    }

    @Override
    public String getInputString() {
        return inputQueryString;
    }

    public ContinuousQuery getParsedContinuousQuery(){
        return parsedCQ;
    }

    @Override
    public IQ translate(InputQueryTranslator translator)
            throws OntopUnsupportedInputQueryException, OntopInvalidInputQueryException {
        if (!(translator instanceof RSP4JInputQueryTranslator)) {
            throw new IllegalArgumentException("RDF4JInputQueryImpl requires an RDF4JInputQueryTranslator");
        }

        return ((RSP4JInputQueryTranslator) translator).translate(parsedQuery);
    }
}
