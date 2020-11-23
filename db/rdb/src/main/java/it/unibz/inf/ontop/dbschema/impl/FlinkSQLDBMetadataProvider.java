package it.unibz.inf.ontop.dbschema.impl;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import it.unibz.inf.ontop.dbschema.*;
import it.unibz.inf.ontop.exception.MetadataExtractionException;
import it.unibz.inf.ontop.model.type.TypeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

public class FlinkSQLDBMetadataProvider extends  DefaultDBMetadataProvider{

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultDBMetadataProvider.class);

    private final QuotedID defaultSchema;
    private final DatabaseMetaData metadata;

    @AssistedInject
    FlinkSQLDBMetadataProvider(@Assisted Connection connection, TypeFactory typeFactory) throws MetadataExtractionException {
        super(connection, getQuotedIDFactory(connection), typeFactory);
        try {
            this.metadata = connection.getMetaData();
            System.out.println("SCHEMA: "+connection.getSchema());
            defaultSchema = retrieveDefaultSchema("SHOW DATABASES"); //OR SHOW DATABASES?????
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
