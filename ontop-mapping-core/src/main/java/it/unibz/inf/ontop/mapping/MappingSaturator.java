package it.unibz.inf.ontop.mapping;


import it.unibz.inf.ontop.model.DBMetadata;
import it.unibz.inf.ontop.owlrefplatform.core.dagjgrapht.TBoxReasoner;

public interface MappingSaturator {

    Mapping saturate(Mapping mapping, DBMetadata dbMetadata, TBoxReasoner saturatedTBox);
}
