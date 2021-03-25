package it.unibz.inf.ontop.answering.reformulation;

import it.polimi.deib.sr.rsp.api.querying.ContinuousQuery;
import it.unibz.inf.ontop.answering.logging.QueryLogger;
import it.unibz.inf.ontop.answering.reformulation.input.InputQuery;
import it.unibz.inf.ontop.exception.OntopReformulationException;
import it.unibz.inf.ontop.iq.IQ;

public interface RSPQueryReformulator extends QueryReformulator {

    IQ reformulateIntoNativeQuery(InputQuery inputQuery, QueryLogger queryLogger, ContinuousQuery parsedCQ) throws OntopReformulationException;
}
