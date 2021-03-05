package it.unibz.inf.ontop.answering.reformulation.input.impl;

import it.unibz.inf.ontop.answering.reformulation.input.InputQuery;
import it.unibz.inf.ontop.answering.reformulation.input.translation.InputQueryTranslator;
import it.unibz.inf.ontop.answering.reformulation.input.translation.RDF4JInputQueryTranslator;
import it.unibz.inf.ontop.answering.reformulation.input.translation.RSP4JInputQueryTranslator;
import it.unibz.inf.ontop.answering.resultset.OBDAResultSet;
import it.unibz.inf.ontop.exception.OntopInvalidInputQueryException;
import it.unibz.inf.ontop.exception.OntopUnsupportedInputQueryException;
import it.unibz.inf.ontop.iq.IQ;
import org.eclipse.rdf4j.query.parser.ParsedQuery;

class RSP4JInputQuery<R extends OBDAResultSet> implements InputQuery<R> {

    protected final ParsedQuery parsedQuery;
    private final String inputQueryString;

    RSP4JInputQuery(ParsedQuery parsedQuery, String inputQueryString) {
        this.parsedQuery = parsedQuery;
        this.inputQueryString = inputQueryString;
    }

    @Override
    public String getInputString() {
        return inputQueryString;
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
