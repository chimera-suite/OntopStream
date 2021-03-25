package it.unibz.inf.ontop.generation.serializer;

import it.unibz.inf.ontop.generation.algebra.SelectFromWhereWithModifiers;
import it.unibz.inf.ontop.dbschema.DBParameters;
import it.polimi.deib.sr.rsp.api.querying.ContinuousQuery;

public interface RSPSelectFromWhereSerializer extends SelectFromWhereSerializer{

    QuerySerialization serialize(SelectFromWhereWithModifiers selectFromWhere, DBParameters dbParameters, ContinuousQuery parsedCQ);

}
