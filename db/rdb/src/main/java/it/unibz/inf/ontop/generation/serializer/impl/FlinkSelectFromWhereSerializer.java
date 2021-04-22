package it.unibz.inf.ontop.generation.serializer.impl;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import it.polimi.deib.sr.rsp.api.operators.s2r.syntax.WindowNode;
import it.polimi.deib.sr.rsp.api.querying.ContinuousQuery;
import it.polimi.deib.sr.rsp.api.stream.web.WebStream;
import it.unibz.inf.ontop.dbschema.*;
import it.unibz.inf.ontop.generation.algebra.SQLExpression;
import it.unibz.inf.ontop.generation.algebra.SelectFromWhereWithModifiers;
import it.unibz.inf.ontop.generation.serializer.*;
import it.unibz.inf.ontop.model.term.*;
import it.unibz.inf.ontop.model.term.functionsymbol.db.DBFunctionSymbol;
import it.unibz.inf.ontop.model.type.DBTermType;
import it.unibz.inf.ontop.substitution.ImmutableSubstitution;
import it.unibz.inf.ontop.utils.ImmutableCollectors;

import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;


import com.google.common.collect.*;
import it.unibz.inf.ontop.generation.algebra.*;
import it.unibz.inf.ontop.generation.serializer.SQLSerializationException;

import java.util.concurrent.atomic.AtomicInteger;

@Singleton
public class FlinkSelectFromWhereSerializer implements RSPSelectFromWhereSerializer {

    protected final FlinkSQLTermSerializer sqlTermSerializer;

    @Inject
    private FlinkSelectFromWhereSerializer(TermFactory termFactory) {
        this(new FlinkSQLTermSerializer(termFactory));
    }

    protected FlinkSelectFromWhereSerializer(FlinkSQLTermSerializer sqlTermSerializer) {
        this.sqlTermSerializer = sqlTermSerializer;
    }

    @Override
    public QuerySerialization serialize(SelectFromWhereWithModifiers selectFromWhere, DBParameters dbParameters) {
        throw new UnsupportedOperationException();
    }

    @Override
    public QuerySerialization serialize(SelectFromWhereWithModifiers selectFromWhere, DBParameters dbParameters, ContinuousQuery parsedCQ){
        return selectFromWhere.acceptVisitor(
                new FlinkRelationVisitingSerializer(dbParameters.getQuotedIDFactory(), parsedCQ));
    }

    /**
     * Mutable: one instance per SQL query to generate
     */
    protected class FlinkRelationVisitingSerializer implements SQLRelationVisitor<QuerySerialization> {

        private static final String VIEW_PREFIX = "v";
        private static final String SELECT_FROM_WHERE_MODIFIERS_TEMPLATE = "SELECT %s%s\nFROM %s\n%s%s%s%s";

        protected final QuotedIDFactory idFactory;
        private final AtomicInteger viewCounter;

        private ContinuousQuery parsedCQ;


        protected FlinkRelationVisitingSerializer(QuotedIDFactory idFactory, ContinuousQuery parsedCQ) {
            this.idFactory = idFactory;
            this.viewCounter = new AtomicInteger(0);
            this.parsedCQ = parsedCQ;
        }

        @Override
        public QuerySerialization visit(SelectFromWhereWithModifiers selectFromWhere) {

            QuerySerialization fromQuerySerialization = getSQLSerializationForChild(selectFromWhere.getFromSQLExpression());

            ImmutableMap<Variable, QuotedID> variableAliases = createVariableAliases(selectFromWhere.getProjectedVariables());

            String distinctString = selectFromWhere.isDistinct() ? "DISTINCT " : "";

            ImmutableMap<Variable, QualifiedAttributeID> columnIDs = fromQuerySerialization.getColumnIDs();
            String projectionString = serializeProjection(selectFromWhere.getProjectedVariables(),
                    variableAliases, selectFromWhere.getSubstitution(), columnIDs);

            String fromString = fromQuerySerialization.getString();
            Boolean isLeafQuery = ((QuerySerializationImpl) fromQuerySerialization).isLeafQuery();

            // TODO: if selectFromWhere.getLimit is 0, then replace it with an additional filter 0 = 1
            String whereString = selectFromWhere.getWhereExpression()
                    .map(e -> sqlTermSerializer.serialize(e, columnIDs))
                    .map(s -> String.format("WHERE %s\n", s))
                    .orElse("");

            String groupByString = serializeGroupBy(selectFromWhere.getGroupByVariables(), columnIDs, /*fromString*/ selectFromWhere.getFromSQLExpression(), isLeafQuery);

            String orderByString = serializeOrderBy(selectFromWhere.getSortConditions(), columnIDs);
            String sliceString = serializeSlice(selectFromWhere.getLimit(), selectFromWhere.getOffset());

            String sql = String.format(SELECT_FROM_WHERE_MODIFIERS_TEMPLATE, distinctString, projectionString,
                    fromString, whereString, groupByString, orderByString, sliceString);

            // Creates an alias for this SQLExpression and uses it for the projected columns
            RelationID alias = generateFreshViewAlias();
            System.out.println("---------------------------FLINK QUERY----------------------------"); //todo: remove debug
            System.out.println(sql);
            System.out.println("------------------------------------------------------------------");
            return new QuerySerializationImpl(sql, attachRelationAlias(alias, variableAliases));
        }

        protected RelationID generateFreshViewAlias() {
            return idFactory.createRelationID(null, VIEW_PREFIX + viewCounter.incrementAndGet());
        }

        private ImmutableMap<Variable, QualifiedAttributeID> attachRelationAlias(RelationID alias, ImmutableMap<Variable, QuotedID> variableAliases) {
            return variableAliases.entrySet().stream()
                    .collect(ImmutableCollectors.toMap(
                            Map.Entry::getKey,
                            e -> new QualifiedAttributeID(alias, e.getValue())));
        }

        private ImmutableMap<Variable, QualifiedAttributeID> replaceRelationAlias(RelationID alias, ImmutableMap<Variable, QualifiedAttributeID> columnIDs) {
            return columnIDs.entrySet().stream()
                    .collect(ImmutableCollectors.toMap(
                            Map.Entry::getKey,
                            e -> new QualifiedAttributeID(alias, e.getValue().getAttribute())));
        }

        private ImmutableMap<Variable, QuotedID> createVariableAliases(ImmutableSet<Variable> variables) {
            AttributeAliasFactory aliasFactory = createAtttibuteAliasFactory();
            return variables.stream()
                    .collect(ImmutableCollectors.toMap(
                            Function.identity(),
                            v -> aliasFactory.createAttributeAlias(v.getName())));
        }

        protected AttributeAliasFactory createAtttibuteAliasFactory() {
            return new DefaultAttributeAliasFactory(idFactory);
        }

        protected String serializeDummyTable() {
            return "";
        }

        protected String serializeProjection(ImmutableSortedSet<Variable> projectedVariables, // only for ORDER
                                             ImmutableMap<Variable, QuotedID> variableAliases,
                                             ImmutableSubstitution<? extends ImmutableTerm> substitution,
                                             ImmutableMap<Variable, QualifiedAttributeID> columnIDs) {

            if (projectedVariables.isEmpty())
                return "1 AS uselessVariable";

            return projectedVariables.stream()
                    .map(v -> sqlTermSerializer.serialize(
                            Optional.ofNullable((ImmutableTerm)substitution.get(v)).orElse(v),
                            columnIDs, substitution.get(v))
                            + " AS " + variableAliases.get(v).getSQLRendering())
                    .collect(Collectors.joining(", "));
        }

        /**
         * FlinkSQL GROUP WINDOWS: https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/sql/queries.html#group-windows
         *
         * TUMBLE(time_attr, interval): Defines a tumbling time window. A tumbling time window assigns rows to
         *  non-overlapping, continuous windows with a fixed duration (interval).
         *
         * HOP(time_attr, interval, interval): Defines a hopping time window (called sliding window in the Table API).
         *  A hopping time window has a fixed duration (second interval parameter) and hops by a specified hop
         *  interval (first interval parameter). If the hop interval is smaller than the window size, hopping windows
         *  are overlapping. Thus, rows can be assigned to multiple windows.
         */
        protected String serializeGroupBy(ImmutableSet<Variable> groupByVariables,
                                          ImmutableMap<Variable, QualifiedAttributeID> columnIDs,
                                          //String fromString, TODO: REMOVE (OLD)
                                          SQLExpression fromSQLExpression,
                                          Boolean isLeafQuery) {
            if(isLeafQuery && !(parsedCQ.getWindowMap().isEmpty())){

                //TODO: evaluate which window is the right one to use
                Map<WindowNode, WebStream> windowMap = (Map<WindowNode, WebStream>) parsedCQ.getWindowMap();
                /*windowMap.forEach((k, v) -> System.out.println((k.iri() + " "+ millisecondToFlinkTime(k.getRange()) + " " +k.getStep()+" | "
                        + v.uri())));*/
                Map.Entry<WindowNode, WebStream> entry = windowMap.entrySet().iterator().next();
                WindowNode node = entry.getKey();
                //WebStream value = entry.getValue();

                String variableString = columnIDs.entrySet()
                        .stream()
                        .map(e -> e.getValue().toString())
                        .collect(Collectors.joining(","));

                /*ArrayList<String> rowTimes = new ArrayList<>(); TODO: REMOVE (OLD)
                rowTimes= getRowTimes(extractTables(fromString));*/
                QuotedID rowTime = extractRowtime(fromSQLExpression);

                // Debug prints TODO: REMOVE (OLD)
                /*System.out.println(variableString + "  --->   ROWTIMES: " + rowTimes);
                System.out.println("RANGE-TIME: "+millisecondToFlinkTime(node.getRange()));*/

                String range = millisecondToFlinkTime(node.getRange());
                String step = millisecondToFlinkTime(node.getStep());
                if(range.equals(step)){  // tumble window
                    variableString += ",TUMBLE("+rowTime+", INTERVAL " + range + ")";
                    /*for (int i = 0; i < rowTimes.size(); i++){
                        variableString += ",TUMBLE("+rowTimes.get(i)+", INTERVAL " + range + ")"; TODO: REMOVE (OLD)
                    }*/
                } else { //hopping window
                    variableString += ",HOP("+rowTime+", INTERVAL " + step + ", INTERVAL " + range + ")";
                    /*for (int i = 0; i < rowTimes.size(); i++){
                        variableString += ",HOP("+rowTimes.get(i)+", INTERVAL " + step + ", INTERVAL " + range + ")"; TODO: REMOVE (OLD)
                    }*/
                }

                return String.format("GROUP BY %s\n", variableString);
            }

            if (groupByVariables.isEmpty())
                return "";

            String variableString = groupByVariables.stream()
                    .map(v -> sqlTermSerializer.serialize(v, columnIDs))
                    .collect(Collectors.joining(", "));

            return String.format("GROUP BY %s\n", variableString);
        }

        private QuotedID extractRowtime(SQLExpression expression){
            String name = ((DatabaseRelationDefinition) ((SQLTable)expression).getRelationDefinition()).getRowtime().get().getName();
            return idFactory.createAttributeID(name);
        }

        /*private ArrayList<String> extractTables(String fromString){  TODO: REMOVE (OLD)
            ArrayList<String> tables = new ArrayList<>();
            String[] parts = fromString.split(" ");

            for( int i = 0; i <= parts.length - 1; i=i+2)
                tables.add(parts[i]);
            return tables;
        }

        private ArrayList<String> getRowTimes(ArrayList<String> tables) {   TODO: REMOVE (OLD)
            ArrayList<String> rowTimes = new ArrayList<>();

            for (int i = 0; i < tables.size(); i++){
                if (tables.get(i).equals("`Rides`")) {
                    rowTimes.add(new String("`rideTime`"));
                } else if (tables.get(i).equals("`Fares`")) {
                    rowTimes.add(new String("`payTime`"));
                } else if (tables.get(i).equals("`DriverChanges`")) {
                    rowTimes.add(new String("`usageStartTime`"));
                } else {
                    throw new UnsupportedOperationException("WRONG TABLE!!!");
                }
            }
            return  rowTimes;
        }*/

        private String millisecondToFlinkTime(long time){
            String flinkTime = "";
            final long DAY = 86400000;
            final long HOUR = 3600000;
            final long MINUTE = 60000;
            final long SECOND = 1000;

            if(time >= DAY){
                int days = (int) Math.floor(time / DAY);
                time = time - (days * DAY);
                flinkTime = flinkTime + ((time != 0) ? ("'" + days + "'" + " DAY,") : ("'" + days + "'" + " DAY"));
            }
            if(time >= HOUR){
                int hours = (int) Math.floor(time / HOUR);
                time = time - (hours * HOUR);
                flinkTime = flinkTime + ((time != 0) ? ("'" + hours + "'" + " HOUR,") : ("'" + hours + "'" + " HOUR"));
            }
            if(time >= MINUTE){
                int minutes = (int) Math.floor(time / MINUTE);
                time = time - (minutes * MINUTE);
                flinkTime = flinkTime + ((time != 0) ? ("'" + minutes + "'" + " MINUTE,") : ("'" + minutes + "'" + " MINUTE"));
            }
            if(time >= SECOND){
                int seconds = (int) Math.floor(time / SECOND);
                time = time - (seconds * SECOND);
                flinkTime = flinkTime + "'" + seconds + "'" + " SECOND";
            }
            return flinkTime;
        }

        protected String serializeOrderBy(ImmutableList<SQLOrderComparator> sortConditions,
                                          ImmutableMap<Variable, QualifiedAttributeID> columnIDs) {
            if (sortConditions.isEmpty())
                return "";

            String conditionString = sortConditions.stream()
                    .map(c -> sqlTermSerializer.serialize(c.getTerm(), columnIDs)
                            + (c.isAscending() ? " NULLS FIRST" : " DESC NULLS LAST"))
                    .collect(Collectors.joining(", "));

            return String.format("ORDER BY %s\n", conditionString);
        }

        /**
         * There is no standard for these three methods (may not work with many DB engines).
         */
        protected String serializeLimitOffset(long limit, long offset) {
            return String.format("LIMIT %d, %d", offset, limit);
        }

        protected String serializeLimit(long limit) {
            return String.format("LIMIT %d", limit);
        }

        protected String serializeOffset(long offset) {
            return String.format("OFFSET %d", offset);
        }


        @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
        private String serializeSlice(Optional<Long> limit, Optional<Long> offset) {
            if (!limit.isPresent() && !offset.isPresent())
                return "";

            if (limit.isPresent() && offset.isPresent())
                return serializeLimitOffset(limit.get(), offset.get());

            if (limit.isPresent())
                return serializeLimit(limit.get());

            return serializeOffset(offset.get());
        }


        @Override
        public QuerySerialization visit(SQLSerializedQuery sqlSerializedQuery) {
            RelationID alias = generateFreshViewAlias();
            String sql = String.format("(%s) %s",sqlSerializedQuery.getSQLString(), alias.getSQLRendering());
            return new QuerySerializationImpl(sql, attachRelationAlias(alias, sqlSerializedQuery.getColumnNames()));
        }

        @Override
        public QuerySerialization visit(SQLTable sqlTable) {
            RelationID alias = generateFreshViewAlias();
            RelationDefinition relation = sqlTable.getRelationDefinition();
            String relationRendering = relation.getAtomPredicate().getName();
            String sql = String.format("%s %s", relationRendering, alias.getSQLRendering());
            return new QuerySerializationImpl(sql, attachRelationAlias(alias, sqlTable.getArgumentMap().entrySet().stream()
                    .collect(ImmutableCollectors.toMap(
                            // Ground terms must have been already removed from atoms
                            e -> (Variable) e.getValue(),
                            e -> relation.getAttribute(e.getKey() + 1).getID()))));
        }

        @Override
        public QuerySerialization visit(SQLNaryJoinExpression sqlNaryJoinExpression) {
            ImmutableList<QuerySerialization> querySerializationList = sqlNaryJoinExpression.getJoinedExpressions().stream()
                    .map(this::getSQLSerializationForChild)
                    .collect(ImmutableCollectors.toList());

            String sql = querySerializationList.stream()
                    .map(QuerySerialization::getString)
                    .collect(Collectors.joining(", "));

            ImmutableMap<Variable, QualifiedAttributeID> columnIDs = querySerializationList.stream()
                    .flatMap(m -> m.getColumnIDs().entrySet().stream())
                    .collect(ImmutableCollectors.toMap());

            return new QuerySerializationImpl(sql, columnIDs);
        }

        @Override
        public QuerySerialization visit(SQLUnionExpression sqlUnionExpression) {
            ImmutableList<QuerySerialization> querySerializationList = sqlUnionExpression.getSubExpressions().stream()
                    .map(e -> e.acceptVisitor(this))
                    .collect(ImmutableCollectors.toList());

            RelationID alias = generateFreshViewAlias();
            String sql = String.format("(%s) %s", querySerializationList.stream()
                    .map(QuerySerialization::getString)
                    .collect(Collectors.joining("UNION ALL \n")), alias.getSQLRendering());

            return new QuerySerializationImpl(sql,
                    replaceRelationAlias(alias, querySerializationList.get(0).getColumnIDs()));
        }

        //this function is required in case at least one of the children is
        // SelectFromWhereWithModifiers expression
        private QuerySerialization getSQLSerializationForChild(SQLExpression expression) {
            if (expression instanceof SelectFromWhereWithModifiers) {
                QuerySerialization serialization = expression.acceptVisitor(this);
                RelationID alias = generateFreshViewAlias();
                String sql = String.format("LATERAL (%s) %s", serialization.getString(), alias.getSQLRendering());
                return new QuerySerializationImpl(sql,
                        replaceRelationAlias(alias, serialization.getColumnIDs()));
            }
            return expression.acceptVisitor(this);
        }

        @Override
        public QuerySerialization visit(SQLInnerJoinExpression sqlInnerJoinExpression) {
            return visit(sqlInnerJoinExpression, "JOIN");
        }

        @Override
        public QuerySerialization visit(SQLLeftJoinExpression sqlLeftJoinExpression) {
            return visit(sqlLeftJoinExpression, "LEFT OUTER JOIN");
        }

        /**
         * NB: the systematic use of ON conditions for inner and left joins saves us from putting parentheses.
         *
         * Indeed since a join expression with a ON is always "CHILD_1 SOME_JOIN CHILD_2 ON COND",
         * the decomposition is unambiguous just following this pattern.
         *
         * For instance, "T1 LEFT JOIN T2 INNER JOIN T3 ON 1=1 ON 2=2"
         * is clearly equivalent to "T1 LEFT JOIN (T2 INNER JOIN T3)"
         * as the latest ON condition ("ON 2=2") can only be attached to the left join, which means that "T2 INNER JOIN T3 ON 1=1"
         * is the right child of the left join.
         *
         */
        protected QuerySerialization visit(BinaryJoinExpression binaryJoinExpression, String operatorString) {
            QuerySerialization left = getSQLSerializationForChild(binaryJoinExpression.getLeft());
            QuerySerialization right = getSQLSerializationForChild(binaryJoinExpression.getRight());

            ImmutableMap<Variable, QualifiedAttributeID> columnIDs = ImmutableList.of(left,right).stream()
                    .flatMap(m -> m.getColumnIDs().entrySet().stream())
                    .collect(ImmutableCollectors.toMap());

            String onString = binaryJoinExpression.getFilterCondition()
                    .map(e -> sqlTermSerializer.serialize(e, columnIDs))
                    .map(s -> String.format("ON %s ", s))
                    .orElse("ON 1 = 1 ");

            String sql = String.format("%s\n %s \n%s %s", left.getString(), operatorString, right.getString(), onString);
            return new QuerySerializationImpl(sql, columnIDs);
        }

        @Override
        public QuerySerialization visit(SQLOneTupleDummyQueryExpression sqlOneTupleDummyQueryExpression) {
            String fromString = serializeDummyTable();
            String sqlSubString = String.format("(SELECT 1 %s) tdummy", fromString);
            return new QuerySerializationImpl(sqlSubString, ImmutableMap.of());
        }
    }


    protected static class QuerySerializationImpl implements QuerySerialization {

        private final String string;
        private final ImmutableMap<Variable, QualifiedAttributeID> columnIDs;

        public QuerySerializationImpl(String string, ImmutableMap<Variable, QualifiedAttributeID> columnIDs) {
            this.string = string;
            this.columnIDs = columnIDs;
        }

        public boolean isLeafQuery(){
            if(!string.contains("SELECT") && !string.contains("FROM") && !string.contains("WHERE")) {
                return Boolean.TRUE;
            }
            return Boolean.FALSE;
        }

        @Override
        public String getString() {
            return string;
        }

        @Override
        public ImmutableMap<Variable, QualifiedAttributeID> getColumnIDs() {
            return columnIDs;
        }
    }


    protected static class FlinkSQLTermSerializer {

        private final TermFactory termFactory;

        protected FlinkSQLTermSerializer(TermFactory termFactory) {
            this.termFactory = termFactory;
        }

        public String serialize(ImmutableTerm term, ImmutableMap<Variable, QualifiedAttributeID> columnIDs)
                throws SQLSerializationException {
            if (term instanceof Constant) {
                return serializeConstant((Constant)term);
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
                                t2 -> serialize(t2, columnIDs),
                                termFactory))
                        .orElseThrow(() -> new SQLSerializationException("Only DBFunctionSymbols must be provided " +
                                "to a SQLTermSerializer"));
            }
        }

        private String serializeConstant(Constant constant) {
            if (constant.isNull())
                return constant.getValue();
            if (!(constant instanceof DBConstant)) {
                throw new SQLSerializationException(
                        "Only DBConstants or NULLs are expected in sub-tree to be translated into SQL");
            }
            return serializeDBConstant((DBConstant) constant);
        }

        public String serialize(ImmutableTerm term, ImmutableMap<Variable, QualifiedAttributeID> columnIDs, ImmutableTerm substitutionTerm)
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
                    output = "BIGINT";
                    break;
            }
            return output;
        }
    }
}

