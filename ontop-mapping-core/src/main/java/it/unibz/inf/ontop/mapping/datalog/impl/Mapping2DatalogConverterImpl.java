package it.unibz.inf.ontop.mapping.datalog.impl;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import it.unibz.inf.ontop.mapping.Mapping;
import it.unibz.inf.ontop.mapping.datalog.Mapping2DatalogConverter;
import it.unibz.inf.ontop.model.CQIE;
import it.unibz.inf.ontop.owlrefplatform.core.translator.IntermediateQueryToDatalogTranslator;
import it.unibz.inf.ontop.pivotalrepr.IntermediateQuery;
import it.unibz.inf.ontop.pivotalrepr.tools.QueryUnionSplitter;

import java.util.List;
import java.util.stream.Stream;

@Singleton
public class Mapping2DatalogConverterImpl implements Mapping2DatalogConverter {

    private final QueryUnionSplitter unionSplitter;

    @Inject
    private Mapping2DatalogConverterImpl(QueryUnionSplitter unionSplitter) {
        this.unionSplitter = unionSplitter;
    }

    @Override
    public Stream<CQIE> convert(Mapping mapping) {
        return mapping.getQueries().stream()
                .flatMap(this::convertMappingQuery);
    }

    private Stream<CQIE> convertMappingQuery(IntermediateQuery mappingQuery) {
        return unionSplitter.splitUnion(mappingQuery)
                .map(this::convertSimpleQuery);
    }

    private CQIE convertSimpleQuery(IntermediateQuery simpleQuery) {
        List<CQIE> rules = IntermediateQueryToDatalogTranslator.translate(simpleQuery).getRules();

        switch (rules.size()) {
            case 0:
                throw new IllegalStateException("No datalog produced for " + simpleQuery);
            case 1:
                return rules.get(0);
            default:
                throw new IllegalStateException("The conversion of a simple query to Datalog is too complex: " +
                        "it should not produce more than one rule. \n" + simpleQuery + "\n" + rules);
        }
    }
}
