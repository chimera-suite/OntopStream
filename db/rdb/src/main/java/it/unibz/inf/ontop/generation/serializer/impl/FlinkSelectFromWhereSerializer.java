package it.unibz.inf.ontop.generation.serializer.impl;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import it.unibz.inf.ontop.dbschema.Attribute;
import it.unibz.inf.ontop.dbschema.DBParameters;
import it.unibz.inf.ontop.dbschema.QualifiedAttributeID;
import it.unibz.inf.ontop.dbschema.QuotedID;
import it.unibz.inf.ontop.dbschema.impl.CachingMetadataLookup;
import it.unibz.inf.ontop.dbschema.impl.FlinkSQLDBMetadataProvider;
import it.unibz.inf.ontop.generation.algebra.SelectFromWhereWithModifiers;
import it.unibz.inf.ontop.generation.serializer.SQLSerializationException;
import it.unibz.inf.ontop.generation.serializer.SelectFromWhereSerializer;
import it.unibz.inf.ontop.model.term.*;
import it.unibz.inf.ontop.model.term.functionsymbol.db.DBFunctionSymbol;
import it.unibz.inf.ontop.model.term.impl.DBConstantImpl;
import it.unibz.inf.ontop.model.type.DBTermType;
import it.unibz.inf.ontop.substitution.ImmutableSubstitution;

import java.util.Optional;
import java.util.stream.Collectors;

@Singleton
public class FlinkSelectFromWhereSerializer extends DefaultSelectFromWhereSerializer implements SelectFromWhereSerializer {

    private final TermFactory termFactory;

    @Inject
    private FlinkSelectFromWhereSerializer(TermFactory termFactory) {
        super(new DefaultSQLTermSerializer(termFactory));
        this.termFactory = termFactory;
    }

    @Override
    public SelectFromWhereSerializer.QuerySerialization serialize(SelectFromWhereWithModifiers selectFromWhere, DBParameters dbParameters) {
        return selectFromWhere.acceptVisitor(
                new DefaultSelectFromWhereSerializer.DefaultRelationVisitingSerializer(dbParameters.getQuotedIDFactory()) {

                    @Override
                    protected String serializeProjection(ImmutableSortedSet<Variable> projectedVariables, // only for ORDER
                                                         ImmutableMap<Variable, QuotedID> variableAliases,
                                                         ImmutableSubstitution<? extends ImmutableTerm> substitution,
                                                         ImmutableMap<Variable, QualifiedAttributeID> columnIDs) {

                        if (projectedVariables.isEmpty())
                            return "1 AS uselessVariable";

                        return projectedVariables.stream()
                                .map(v -> serialize(
                                        Optional.ofNullable((ImmutableTerm)substitution.get(v)).orElse(v),
                                        columnIDs, substitution.get(v))
                                        + " AS " + variableAliases.get(v).getSQLRendering())
                                .collect(Collectors.joining(", "));
                    }


                    private String serialize(ImmutableTerm term, ImmutableMap<Variable, QualifiedAttributeID> columnIDs, ImmutableTerm substitutionTerm)
                            throws SQLSerializationException {
                        if (term instanceof Constant) {
                            return serializeConstant((Constant)term, substitutionTerm);
                        }
                        else if (term instanceof Variable) {
                            return Optional.ofNullable(columnIDs.get(term))
                                    .map(QualifiedAttributeID::getSQLRendering)
                                    .orElseThrow(() -> new SQLSerializationException(String.format(
                                            "The variable %s does not appear in the columnIDs", term)));
                        }
                        /*
                         * ImmutableFunctionalTerm with a DBFunctionSymbol
                         */
                        else {
                            return Optional.of(term)
                                    .filter(t -> t instanceof ImmutableFunctionalTerm)
                                    .map(t -> (ImmutableFunctionalTerm) t)
                                    .filter(t -> t.getFunctionSymbol() instanceof DBFunctionSymbol)
                                    .map(t -> ((DBFunctionSymbol) t.getFunctionSymbol()).getNativeDBString(
                                            t.getTerms(),
                                            t2 -> serialize(t2, columnIDs, substitutionTerm),
                                            termFactory))
                                    .orElseThrow(() -> new SQLSerializationException("Only DBFunctionSymbols must be provided " +
                                            "to a SQLTermSerializer"));
                        }
                    }

                    private String serializeConstant(Constant constant, ImmutableTerm substitutionTerm) {
                        if (constant.isNull()) {
                            return "CAST(NULL AS " + nullDatatype(substitutionTerm) + ")";
                        }

                        if (!(constant instanceof DBConstant)) {
                            throw new SQLSerializationException(
                                    "Only DBConstants or NULLs are expected in sub-tree to be translated into SQL");
                        }
                        return serializeDBConstant((DBConstant) constant);
                    }

                    protected String serializeDBConstant(DBConstant constant) {
                        DBTermType dbType = constant.getType();

                        switch (dbType.getCategory()) {
                            case DECIMAL:
                            case FLOAT_DOUBLE:
                                // TODO: handle the special case of not-a-number!
                                return castFloatingConstant(constant.getValue(), dbType);
                            case INTEGER:
                            case BOOLEAN:
                                return constant.getValue();
                            default:
                                return serializeStringConstant(constant.getValue());
                        }
                    }

                    protected String castFloatingConstant(String value, DBTermType dbType) {
                        return String.format("CAST(%s AS %s)", value, dbType.getCastName());
                    }

                    protected String serializeStringConstant(String constant) {
                        // duplicates single quotes, and adds outermost quotes
                        return "'" + constant.replace("'", "''") + "'";
                    }

                });
    }

    /*
     * NULL datatypes name conversion
     * Java --> FlinkSQL datatypes: https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/types.html#planner-compatibility
     * TODO: check all the castings
     */
    private String nullDatatype(ImmutableTerm substitutionTerm){
        String output = substitutionTerm.toString().replaceAll("(NULL-|\\(\\))","");

        //Manage the cases where the Java datatype name is different from the Flink SQL datatype
        switch(output){
            case "BYTE":
                output = "TINYINT";
                break;
            case "SHORT":
                output = "SMALLINT";
                break;
            case "LONG":
                System.out.println("SUBSTITUTED: "+output);
                output = "BIGINT";
                System.out.println("SUBSTITUTED: "+output);
                break;
        }
        return output;
    }
}
