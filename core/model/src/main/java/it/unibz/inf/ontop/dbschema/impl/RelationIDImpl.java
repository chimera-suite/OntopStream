package it.unibz.inf.ontop.dbschema.impl;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.collect.ImmutableList;
import it.unibz.inf.ontop.dbschema.QuotedID;
import it.unibz.inf.ontop.dbschema.RelationID;

import java.io.IOException;

public class RelationIDImpl implements RelationID {
    private final QuotedID schema, table;
    private QuotedID  rowtime;

    /**
     * (used only in QuotedIDFactory implementations)
     *
     * @param schema
     * @param table
     */

    RelationIDImpl(QuotedID schema, QuotedID table) {
        this.schema = schema;
        this.table = table;
        this.rowtime = null;
    }

    /**
     *
     * @return the relation ID that has the same name but no schema name
     */
    @JsonIgnore
    @Override
    public ImmutableList<RelationID> getWithSchemalessID() {
        return (schema.getName() == null)
                ? ImmutableList.of(this)
                : ImmutableList.of(new RelationIDImpl(QuotedIDImpl.EMPTY_ID, table), this);
    }


    @JsonProperty("name")
    @Override
    public QuotedID getTableID() {
        return table;
    }

    /**
     *
     * @return null if the schema name is empty or the schema name (as is, without quotation marks)
     */
    @JsonProperty("schema")
    @Override
    public QuotedID getSchemaID() {
        return schema;
    }

    /**
     *
     * @return SQL rendering of the name (possibly with quotation marks)
     */
    @JsonIgnore
    @Override
    public String getSQLRendering() {
        String s = schema.getSQLRendering();
        if (s == null)
            return table.getSQLRendering();

        return s + "." + table.getSQLRendering();
    }

    @Override
    public String toString() {
        return getSQLRendering();
    }

    @Override
    public int hashCode() {
        return table.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (obj instanceof RelationIDImpl) {
            RelationIDImpl other = (RelationIDImpl)obj;
            return (this.schema.equals(other.schema) && this.table.equals(other.table));
        }

        return false;
    }

    public void setRowtime(QuotedID attributeId){
        this.rowtime = attributeId;
    }

    public QuotedID getRowtime(){
        return rowtime;
    }

    public static class RelationIDSerializer extends JsonSerializer<RelationID> {

        @Override
        public void serialize(RelationID value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeString(value.getSQLRendering());
        }
    }
}
