package it.unibz.inf.ontop.dbschema.impl;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import it.unibz.inf.ontop.dbschema.*;
import it.unibz.inf.ontop.exception.MetadataExtractionException;
import it.unibz.inf.ontop.model.type.DBTermType;
import it.unibz.inf.ontop.model.type.TypeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class FlinkSQLDBMetadataProvider extends  DefaultDBMetadataProvider{

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultDBMetadataProvider.class);

    private final QuotedID defaultSchema;
    private final DatabaseMetaData metadata;

    @AssistedInject
    FlinkSQLDBMetadataProvider(@Assisted Connection connection, TypeFactory typeFactory) throws MetadataExtractionException {
        super(connection, getQuotedIDFactory(connection), typeFactory);
        try {
            this.metadata = connection.getMetaData();
            defaultSchema = retrieveDefaultSchema("SHOW DATABASES"); //OR SHOW DATABASES?????
        }
        catch (SQLException e) {
            throw new MetadataExtractionException(e);
        }
    }

    @Override
    public DatabaseRelationDefinition getRelation(RelationID id) throws MetadataExtractionException {

        try (ResultSet rs = metadata.getColumns(getRelationCatalog(id), getRelationSchema(id), getRelationName(id), null)) {
            Map<RelationID, RelationDefinition.AttributeListBuilder> relations = new HashMap<>();

            while (rs.next()) {
                RelationID extractedId = getRelationID(rs);
                checkSameRelationID(extractedId, id);

                RelationDefinition.AttributeListBuilder builder = relations.computeIfAbsent(extractedId,
                        i -> DatabaseTableDefinition.attributeListBuilder());

                QuotedID attributeId = rawIdFactory.createAttributeID(rs.getString("COLUMN_NAME"));
                // columnNoNulls, columnNullable, columnNullableUnknown
                boolean isNullable = rs.getInt("NULLABLE") != DatabaseMetaData.columnNoNulls;
                String typeName = rs.getString("TYPE_NAME");
                int columnSize = rs.getInt("COLUMN_SIZE");

                // Set the rowtime for the table. TODO: check with two timestamps in a single table
                if (typeName.contains("TIMESTAMP")) {
                    ((RelationIDImpl) id).setRowtime(attributeId);
                }

                DBTermType termType = dbTypeFactory.getDBTermType(typeName, columnSize);

                builder.addAttribute(attributeId, termType, typeName, isNullable);
            }

            if (relations.isEmpty()) {
                throw new MetadataExtractionException("Cannot find relation id: " + id);
            }
            else if (relations.entrySet().size() == 1) {
                Map.Entry<RelationID, RelationDefinition.AttributeListBuilder> r = relations.entrySet().iterator().next();
                return new DatabaseTableDefinition(getRelationAllIDs(r.getKey()), r.getValue());
            }
            throw new MetadataExtractionException("Cannot resolve ambiguous relation id: " + id + ": " + relations.keySet());
        }
        catch (SQLException e) {
            throw new MetadataExtractionException(e);
        }
    }

    protected static QuotedIDFactory getQuotedIDFactory(Connection connection) throws MetadataExtractionException {
        try {
            DatabaseMetaData md = connection.getMetaData();
            return new FlinkSQLQuotedIDFactory(true);
        }catch (SQLException e) {
            throw new MetadataExtractionException(e);
        }
    }

    @Override
    public void insertIntegrityConstraints(DatabaseRelationDefinition relation, MetadataLookup metadataLookup) throws MetadataExtractionException {
        //insertPrimaryKey(relation);
        //insertUniqueAttributes(relation);
        //insertForeignKeys(relation, metadataLookup);
        //TODO: fix methods
    }

    @Override
    public QuotedID getDefaultSchema() { return defaultSchema; }

    @Override
    protected String getRelationCatalog(RelationID relationID) {
        return "default_catalog"; //TODO: make it dynamic
    }

}
