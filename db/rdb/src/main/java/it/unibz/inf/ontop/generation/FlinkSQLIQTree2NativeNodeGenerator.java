package it.unibz.inf.ontop.generation;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.inject.Inject;
import it.unibz.inf.ontop.generation.serializer.impl.FlinkSelectFromWhereSerializer;
import it.unibz.inf.ontop.generation.algebra.IQTree2SelectFromWhereConverter;
import it.unibz.inf.ontop.generation.algebra.SelectFromWhereWithModifiers;
import it.unibz.inf.ontop.generation.serializer.SelectFromWhereSerializer;
import it.unibz.inf.ontop.dbschema.DBParameters;
import it.unibz.inf.ontop.dbschema.QuotedID;
import it.unibz.inf.ontop.exception.MinorOntopInternalBugException;
import it.unibz.inf.ontop.injection.IntermediateQueryFactory;
import it.unibz.inf.ontop.iq.IQTree;
import it.unibz.inf.ontop.iq.node.NativeNode;
import it.unibz.inf.ontop.iq.transform.RSPIQTree2NativeNodeGenerator;
import it.unibz.inf.ontop.iq.type.UniqueTermTypeExtractor;
import it.unibz.inf.ontop.model.term.Variable;
import it.unibz.inf.ontop.model.type.DBTermType;
import it.unibz.inf.ontop.model.type.TypeFactory;
import it.unibz.inf.ontop.utils.ImmutableCollectors;

import it.polimi.deib.sr.rsp.api.querying.ContinuousQuery;

import java.util.Map;
import java.util.Optional;

public class FlinkSQLIQTree2NativeNodeGenerator implements RSPIQTree2NativeNodeGenerator {

    private final FlinkSelectFromWhereSerializer serializer;
    private final IQTree2SelectFromWhereConverter converter;
    private final IntermediateQueryFactory iqFactory;
    private final UniqueTermTypeExtractor uniqueTermTypeExtractor;
    private final DBTermType abstractRootDBType;

    @Inject
    private FlinkSQLIQTree2NativeNodeGenerator(SelectFromWhereSerializer serializer,
                                                 IQTree2SelectFromWhereConverter converter,
                                                 IntermediateQueryFactory iqFactory,
                                                 UniqueTermTypeExtractor uniqueTermTypeExtractor,
                                                 TypeFactory typeFactory) {
        this.serializer = (FlinkSelectFromWhereSerializer) serializer;
        this.converter = converter;
        this.iqFactory = iqFactory;
        this.uniqueTermTypeExtractor = uniqueTermTypeExtractor;
        abstractRootDBType = typeFactory.getDBTypeFactory().getAbstractRootDBType();
    }

    @Override
    public NativeNode generate(IQTree iqTree, DBParameters dbParameters, boolean tolerateUnknownTypes) {
        System.out.println("ERROR - FlinkSQLIQTree2NativeNodeGenerator.generate()");
        throw new UnsupportedOperationException("FlinkSQLIQTree2NativeNodeGenerator.generate() not supported, please change function call");
    }

    @Override
    public NativeNode generate(IQTree iqTree, DBParameters dbParameters, ContinuousQuery parsedCQ, boolean tolerateUnknownTypes) {
        ImmutableSortedSet<Variable> signature = ImmutableSortedSet.copyOf(iqTree.getVariables());

        System.out.println("--------------- QUERY TREE PSEUDOCODE(FlinkSQLIQTree2NativeNodeGenerator) ---------------");
        System.out.println("\n"+iqTree+"\nSIGNATURE: "+signature);

        //Map<WindowNode, WebStream> iteration
        /*parsedCQ.getWindowMap().forEach((k, v) -> System.out.println((k.iri() + " "+ k.getRange() + " " +k.getStep()+" | "
                + v.uri())));*/

        System.out.println("-----------------------------------------------------------------------------------------");

        SelectFromWhereWithModifiers selectFromWhere = converter.convert(iqTree, signature);
        FlinkSelectFromWhereSerializer.QuerySerialization serializedQuery = serializer.serialize(selectFromWhere, dbParameters, parsedCQ);

        ImmutableMap<Variable, DBTermType> variableTypeMap = extractVariableTypeMap(iqTree, tolerateUnknownTypes);

        ImmutableMap<Variable, QuotedID> columnNames = serializedQuery.getColumnIDs().entrySet().stream()
                .collect(ImmutableCollectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().getAttribute()));

        return iqFactory.createNativeNode(signature, variableTypeMap, columnNames,
                serializedQuery.getString(), iqTree.getVariableNullability());
    }

    private ImmutableMap<Variable, DBTermType> extractVariableTypeMap(IQTree tree, boolean tolerateUnknownTypes) {
        return tree.getVariables().stream()
                .collect(ImmutableCollectors.toMap(
                        v -> v,
                        v -> extractUniqueKnownType(v, tree, tolerateUnknownTypes)));
    }

    private DBTermType extractUniqueKnownType(Variable v, IQTree tree, boolean tolerateUnknownTypes) {
        return uniqueTermTypeExtractor.extractUniqueTermType(v, tree)
                .filter(t -> t instanceof DBTermType)
                .map(t -> (DBTermType) t)
                .map(Optional::of)
                .orElseGet(() -> tolerateUnknownTypes ?
                        Optional.of(abstractRootDBType) : Optional.empty())
                .orElseThrow(() -> new MinorOntopInternalBugException(
                        "Was expecting a unique and known DB term type to be extracted " +
                                "for the SQL variable " + v));
    }
}

