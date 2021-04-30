package it.polimi.deib.sr.rsp.yasper;

import it.polimi.deib.sr.rsp.api.operators.r2r.RelationToRelationOperator;
import it.polimi.deib.sr.rsp.api.operators.r2s.RelationToStreamOperator;
import it.polimi.deib.sr.rsp.api.operators.s2r.execution.assigner.StreamToRelationOp;
import it.polimi.deib.sr.rsp.api.querying.ContinuousQuery;
import it.polimi.deib.sr.rsp.api.querying.result.SolutionMapping;
import it.polimi.deib.sr.rsp.api.sds.SDS;
import it.polimi.deib.sr.rsp.api.stream.data.WebDataStream;
import lombok.extern.log4j.Log4j;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Observable;
import java.util.stream.Stream;

/**
 * Created by Riccardo on 12/08/16.
 */

@Log4j
public class ContinuousQueryExecutionImpl<I, E1, E2> extends ContinuousQueryExecutionObserver<I, E1, E2> {

    private final RelationToStreamOperator<E2> r2s;
    private final RelationToRelationOperator<E2> r2r;
    private final SDS<E1> sds;
    private final ContinuousQuery query;
    private final WebDataStream<E2> outstream;
    private List<StreamToRelationOp<I, E1>> s2rs;

    public ContinuousQueryExecutionImpl(SDS sds, ContinuousQuery query, WebDataStream<E2> outstream, RelationToRelationOperator<E2> r2r, RelationToStreamOperator<E2> r2s, StreamToRelationOp<I, E1>... s2rs) {
        super(sds, query);
        this.s2rs = Arrays.asList(s2rs);
        this.query = query;
        this.sds = sds;
        this.r2r = r2r;
        this.r2s = r2s;
        this.outstream = outstream;
    }

    @Override
    public WebDataStream<E2> outstream() {
        return outstream;
    }

    @Override
    public ContinuousQuery query() {
        return query;
    }

    @Override
    public SDS sds() {
        return sds;
    }

    @Override
    public StreamToRelationOp<I, E1>[] s2rs() {
        StreamToRelationOp<I, E1>[] a = new StreamToRelationOp[s2rs.size()];
        return s2rs.toArray(a);
    }

    @Override
    public RelationToRelationOperator<E2> r2r() {
        return r2r;
    }

    @Override
    public RelationToStreamOperator<E2> r2s() {
        return r2s;
    }

    @Override
    public void add(StreamToRelationOp<I, E1> op) {
        op.link(this);
    }

    @Override
    public void update(Observable o, Object arg) {
        Long now = (Long) arg;
        eval(now).forEach(o1 -> outstream().put(r2s.eval(o1, now), now));
    }

    @Override
    public Stream<SolutionMapping<E2>> eval(Long now) {
        sds.materialize(now);
        return r2r.eval(now);
    }
}

