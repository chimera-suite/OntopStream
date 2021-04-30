package it.polimi.deib.sr.rsp.api.operators.s2r.execution.assigner;


import it.polimi.deib.sr.rsp.api.enums.Tick;
import it.polimi.deib.sr.rsp.api.operators.s2r.execution.instance.Window;
import it.polimi.deib.sr.rsp.api.querying.ContinuousQueryExecution;
import it.polimi.deib.sr.rsp.api.sds.timevarying.TimeVarying;
import it.polimi.deib.sr.rsp.api.secret.content.Content;
import it.polimi.deib.sr.rsp.api.secret.report.Report;
import it.polimi.deib.sr.rsp.api.secret.time.Time;

import java.util.List;

/*
 * I represents the variable type of the input, e.g., RDF TRIPLE, RDF GRAPHS OR TUPLE
 * O represents the variable type of the maintained status, e.g., BAG of RDF Triple, RDF Graph (set) or RELATION
 * */

public interface StreamToRelationOp<I, O> extends Consumer<I> {

    Report report();

    Tick tick();

    Time time();

    Content<I, O> content(long now);

    List<Content<I, O>> getContents(long now);

    TimeVarying<O> get();

    String iri();

    default boolean named() {
        return iri() != null;
    }

    Content<I, O> compute(long t_e, Window w);

    void link(ContinuousQueryExecution context);
}
