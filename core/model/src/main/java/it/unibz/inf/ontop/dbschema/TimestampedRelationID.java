package it.unibz.inf.ontop.dbschema;

public interface TimestampedRelationID extends RelationID{

    void setRowtime(QuotedID attributeId);

    QuotedID getRowtime();
}
