package it.unibz.inf.ontop.model.term.functionsymbol.db.impl;

import com.google.common.collect.ImmutableList;
import it.unibz.inf.ontop.model.term.ImmutableTerm;
import it.unibz.inf.ontop.model.term.TermFactory;
import it.unibz.inf.ontop.model.type.DBTermType;

import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FlinkDBCaseFunctionSymbol extends DefaultDBCaseFunctionSymbol{

    private static final String WHEN_THEN_TEMPLATE = "    WHEN %s THEN %s\n";
    private static final String FULL_TEMPLATE = "CASE %s    ELSE %s \nEND";

    protected FlinkDBCaseFunctionSymbol(int arity, DBTermType dbBooleanType, DBTermType rootDBTermType, boolean doOrderingMatter) {
        super(arity, dbBooleanType, rootDBTermType, doOrderingMatter);
    }

    @Override
    public String getNativeDBString(ImmutableList<? extends ImmutableTerm> terms, Function<ImmutableTerm, String> termConverter, TermFactory termFactory) {
        String whenClauseString = IntStream.range(0, terms.size() / 2)
                .boxed()
                .map(i -> String.format(WHEN_THEN_TEMPLATE,
                        termConverter.apply(terms.get(2*i)),
                        termConverter.apply(terms.get(2*i + 1))))
                .collect(Collectors.joining());

        if(terms.get(terms.size() - 1).isNull()){
            return String.format(FULL_TEMPLATE, whenClauseString, "NULL");
        }
        return String.format(FULL_TEMPLATE, whenClauseString,
                termConverter.apply(terms.get(terms.size() - 1)));
    }
}
