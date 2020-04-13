package it.unibz.inf.ontop.dbschema.impl;

import com.google.common.collect.ImmutableList;
import it.unibz.inf.ontop.dbschema.RelationID;
import it.unibz.inf.ontop.exception.MetadataExtractionException;
import it.unibz.inf.ontop.model.type.DBTypeFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class OracleDBMetadataProvider extends DefaultDBMetadataProvider {

    private final String defaultTableOwner;

    OracleDBMetadataProvider(Connection connection, DBTypeFactory dbTypeFactory) throws MetadataExtractionException {
        super(connection, dbTypeFactory);
        this.defaultTableOwner = getDefaultOwner();
    }

    @Override
    protected String getDefaultSchema() { return defaultTableOwner; }

    private static final String DUAL = "DUAL"; // DUAL is a special Oracle table

    @Override
    protected String getEffectiveRelationSchema(RelationID relationID) {
        if (relationID.getTableID().getName().equals(DUAL))
                return null;

        return super.getEffectiveRelationSchema(relationID);
    }

    @Override
    protected boolean sameRelationID(RelationID extractedId, RelationID givenId)  {
        if (extractedId.getTableID().getName().equals(DUAL))
            return true;

        return super.sameRelationID(extractedId, givenId);
    }


    @Override
    public ImmutableList<RelationID> getRelationAllIDs(RelationID id) {
        if (id.getTableID().getName().equals(DUAL))
            return ImmutableList.of(id.getSchemalessID());

        if (defaultTableOwner.equals(id.getSchemaID().getName()))
            return ImmutableList.of(id.getSchemalessID(), id);

        return ImmutableList.of(id);
    }


    @Override
    public ImmutableList<RelationID> getRelationIDs() throws MetadataExtractionException {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(TABLE_LIST_QUERY)) {
            ImmutableList.Builder<RelationID> relationIds = ImmutableList.builder();
            while (rs.next()) {
                RelationID id = rawIdFactory.createRelationID(
                        defaultTableOwner, rs.getString("object_name"));
                relationIds.add(id);
            }
            return relationIds.build();
        }
        catch (SQLException e) {
            throw new MetadataExtractionException(e);
        }
    }

    // Obtain the relational objects (i.e., tables and views)
    // filter out all irrelevant table and view names
    private static final String TABLE_LIST_QUERY =
        "SELECT table_name as object_name FROM user_tables WHERE " +
                "   NOT table_name LIKE 'MVIEW$_%' AND " +
                "   NOT table_name LIKE 'LOGMNR_%' AND " +
                "   NOT table_name LIKE 'AQ$_%' AND " +
                "   NOT table_name LIKE 'DEF$_%' AND " +
                "   NOT table_name LIKE 'REPCAT$_%' AND " +
                "   NOT table_name LIKE 'LOGSTDBY$%' AND " +
                "   NOT table_name LIKE 'OL$%' " +
                "UNION ALL " +
                "SELECT view_name as object_name FROM user_views WHERE " +
                "   NOT view_name LIKE 'MVIEW_%' AND " +
                "   NOT view_name LIKE 'LOGMNR_%' AND " +
                "   NOT view_name LIKE 'AQ$_%'";

    private String getDefaultOwner() throws MetadataExtractionException {
        // Obtain the table owner (i.e., schema name)
        try (Statement stmt = connection.createStatement();
             ResultSet resultSet = stmt.executeQuery("SELECT user FROM dual")) {
            return (resultSet.next())
                    ? resultSet.getString(1).toUpperCase()
                    : "SYSTEM"; // default value
        }
        catch (SQLException e) {
            throw new MetadataExtractionException(e);
        }
    }
}
