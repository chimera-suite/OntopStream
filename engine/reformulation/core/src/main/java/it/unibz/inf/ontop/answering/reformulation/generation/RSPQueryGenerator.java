package it.unibz.inf.ontop.answering.reformulation.generation;

import it.polimi.deib.sr.rsp.api.querying.ContinuousQuery;
import it.unibz.inf.ontop.iq.IQ;

/**
 * Generates an IQ containing a source query in a given native query language.
 *
 * See TranslationFactory for creating a new instance.
 *
 */
public interface RSPQueryGenerator extends NativeQueryGenerator{

    IQ generateSourceQuery(IQ query, ContinuousQuery parsedCQ);
}
