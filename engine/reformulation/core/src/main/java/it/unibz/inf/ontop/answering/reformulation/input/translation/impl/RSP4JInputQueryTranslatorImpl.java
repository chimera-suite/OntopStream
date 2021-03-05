package it.unibz.inf.ontop.answering.reformulation.input.translation.impl;

import com.google.inject.Inject;
import it.unibz.inf.ontop.answering.reformulation.input.translation.RSP4JInputQueryTranslator;
import it.unibz.inf.ontop.exception.OntopInvalidInputQueryException;
import it.unibz.inf.ontop.exception.OntopUnsupportedInputQueryException;
import it.unibz.inf.ontop.injection.IntermediateQueryFactory;
import it.unibz.inf.ontop.iq.IQ;
import it.unibz.inf.ontop.model.atom.AtomFactory;
import it.unibz.inf.ontop.model.term.TermFactory;
import it.unibz.inf.ontop.model.term.functionsymbol.FunctionSymbolFactory;
import it.unibz.inf.ontop.model.type.TypeFactory;
import it.unibz.inf.ontop.substitution.SubstitutionFactory;
import it.unibz.inf.ontop.utils.CoreUtilsFactory;
import org.apache.commons.rdf.api.RDF;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RSP4JInputQueryTranslatorImpl implements RSP4JInputQueryTranslator {

    private final CoreUtilsFactory coreUtilsFactory;
    private final TermFactory termFactory;
    private final SubstitutionFactory substitutionFactory;
    private final TypeFactory typeFactory;
    private final IntermediateQueryFactory iqFactory;
    private final AtomFactory atomFactory;
    private final RDF rdfFactory;
    private final FunctionSymbolFactory functionSymbolFactory;

    private static final Logger log = LoggerFactory.getLogger(RDF4JInputQueryTranslatorImpl.class);
    private static final boolean IS_DEBUG_ENABLED = log.isDebugEnabled();

    @Inject
    public RSP4JInputQueryTranslatorImpl(CoreUtilsFactory coreUtilsFactory, TermFactory termFactory, SubstitutionFactory substitutionFactory,
                                         TypeFactory typeFactory, IntermediateQueryFactory iqFactory, AtomFactory atomFactory, RDF rdfFactory,
                                         FunctionSymbolFactory functionSymbolFactory) {
        this.coreUtilsFactory = coreUtilsFactory;
        this.termFactory = termFactory;
        this.substitutionFactory = substitutionFactory;
        this.typeFactory = typeFactory;
        this.iqFactory = iqFactory;
        this.atomFactory = atomFactory;
        this.rdfFactory = rdfFactory;
        this.functionSymbolFactory = functionSymbolFactory;
    }

    @Override
    public IQ translate(ParsedQuery inputParsedQuery) throws OntopUnsupportedInputQueryException, OntopInvalidInputQueryException {
        throw new UnsupportedOperationException("translate not supported!!!");
    }

    @Override
    public IQ translateAskQuery(ParsedQuery parsedQuery) throws OntopUnsupportedInputQueryException, OntopInvalidInputQueryException {
        throw new UnsupportedOperationException("translateAskQuery not supported!!!");
    }
}
