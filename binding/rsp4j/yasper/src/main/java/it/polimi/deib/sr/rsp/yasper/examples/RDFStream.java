package it.polimi.deib.sr.rsp.yasper.examples;

import it.polimi.deib.sr.rsp.api.operators.s2r.execution.assigner.Consumer;
import it.polimi.deib.sr.rsp.api.stream.data.WebDataStream;
import it.polimi.deib.sr.rsp.api.stream.web.WebStreamImpl;
import org.apache.commons.rdf.api.Graph;

import java.util.ArrayList;
import java.util.List;

public class RDFStream implements WebDataStream<Graph> {

    protected String stream_uri;

    public RDFStream(String stream_uri) {
        this.stream_uri = stream_uri;
    }

    protected List<Consumer<Graph>> consumers = new ArrayList<>();


    @Override
    public void addConsumer(Consumer<Graph> c) {
        consumers.add(c);
    }

    @Override
    public void put(Graph e, long ts) {
        consumers.forEach(graphConsumer -> graphConsumer.notify(e, ts));
    }

    @Override
    public String uri() {
        return stream_uri;
    }

}
