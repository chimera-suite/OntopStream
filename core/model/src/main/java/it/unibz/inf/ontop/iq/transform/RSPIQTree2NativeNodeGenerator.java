package it.unibz.inf.ontop.iq.transform;

import it.unibz.inf.ontop.dbschema.DBParameters;
import it.unibz.inf.ontop.iq.IQTree;
import it.unibz.inf.ontop.iq.node.NativeNode;
import it.polimi.deib.sr.rsp.api.querying.ContinuousQuery;

public interface RSPIQTree2NativeNodeGenerator extends IQTree2NativeNodeGenerator{

    NativeNode generate(IQTree iqTree, DBParameters dbParameters, ContinuousQuery parsedCQ, boolean tolerateUnknownTypes);
}
