package it.unibz.inf.ontop.answering.reformulation.generation.impl;


import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import it.unibz.inf.ontop.answering.reformulation.generation.RSPQueryGenerator;
import it.unibz.inf.ontop.iq.transform.IQTree2NativeNodeGenerator;
import it.unibz.inf.ontop.answering.reformulation.generation.PostProcessingProjectionSplitter;
import it.unibz.inf.ontop.generation.normalization.DialectExtraNormalizer;
import it.unibz.inf.ontop.datalog.UnionFlattener;
import it.unibz.inf.ontop.dbschema.DBParameters;
import it.unibz.inf.ontop.injection.IntermediateQueryFactory;
import it.unibz.inf.ontop.injection.OptimizerFactory;
import it.unibz.inf.ontop.iq.IQ;
import it.unibz.inf.ontop.iq.IQTree;
import it.unibz.inf.ontop.iq.UnaryIQTree;
import it.unibz.inf.ontop.iq.node.*;
import it.unibz.inf.ontop.iq.optimizer.PostProcessableFunctionLifter;
import it.unibz.inf.ontop.iq.optimizer.TermTypeTermLifter;
import it.unibz.inf.ontop.iq.transform.RSPIQTree2NativeNodeGenerator;
import it.unibz.inf.ontop.iq.transformer.BooleanExpressionPushDownTransformer;
import it.unibz.inf.ontop.utils.VariableGenerator;
import org.slf4j.LoggerFactory;

import it.polimi.deib.sr.rsp.api.querying.ContinuousQuery;


/**
 * TODO: explain
 *
 * See TranslationFactory for creating a new instance.
 *
 */
public class FlinkSQLGeneratorImpl implements RSPQueryGenerator {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(SQLGeneratorImpl.class);
    private static final boolean IS_DEBUG_ENABLED = log.isDebugEnabled();
    private final DBParameters dbParameters;
    private final IntermediateQueryFactory iqFactory;
    private final UnionFlattener unionFlattener;
    private final OptimizerFactory optimizerFactory;
    private final PostProcessingProjectionSplitter projectionSplitter;
    private final TermTypeTermLifter rdfTypeLifter;
    private final PostProcessableFunctionLifter functionLifter;
    private final RSPIQTree2NativeNodeGenerator RSPIQTree2NativeNodeGenerator;
    private final DialectExtraNormalizer extraNormalizer;
    private final BooleanExpressionPushDownTransformer pushDownTransformer;

    @AssistedInject
    private FlinkSQLGeneratorImpl(@Assisted DBParameters dbParameters,
                             IntermediateQueryFactory iqFactory,
                             UnionFlattener unionFlattener,
                             OptimizerFactory optimizerFactory,
                             PostProcessingProjectionSplitter projectionSplitter,
                             TermTypeTermLifter rdfTypeLifter, PostProcessableFunctionLifter functionLifter,
                             IQTree2NativeNodeGenerator defaultIQTree2NativeNodeGenerator,
                             DialectExtraNormalizer extraNormalizer, BooleanExpressionPushDownTransformer pushDownTransformer)
    {
        this.functionLifter = functionLifter;
        this.extraNormalizer = extraNormalizer;
        this.pushDownTransformer = pushDownTransformer;
        this.dbParameters = dbParameters;
        this.iqFactory = iqFactory;
        this.unionFlattener = unionFlattener;
        this.optimizerFactory = optimizerFactory;
        this.projectionSplitter = projectionSplitter;
        this.rdfTypeLifter = rdfTypeLifter;
        this.RSPIQTree2NativeNodeGenerator = (RSPIQTree2NativeNodeGenerator)defaultIQTree2NativeNodeGenerator;
    }

    @Override
    public IQ generateSourceQuery(IQ query) {
        System.out.println("ERROR - FlinkSQLGeneratorImpl.generateSourceQuery()");
        throw new UnsupportedOperationException("FlinkSQLGeneratorImpl.generateSourceQuery() not supported, please change function call");
    }

    @Override
    public IQ generateSourceQuery(IQ query, ContinuousQuery parsedCQ) {

        if (query.getTree().isDeclaredAsEmpty())
            return query;

        IQ rdfTypeLiftedIQ = rdfTypeLifter.optimize(query);
        if (IS_DEBUG_ENABLED)
            log.debug("After lifting the RDF types:\n" + rdfTypeLiftedIQ);

        IQ liftedIQ = functionLifter.optimize(rdfTypeLiftedIQ);
        if (IS_DEBUG_ENABLED)
            log.debug("After lifting the post-processable function symbols :\n" + liftedIQ);

        PostProcessingProjectionSplitter.PostProcessingSplit split = projectionSplitter.split(liftedIQ);

        IQTree normalizedSubTree = normalizeSubTree(split.getSubTree(), split.getVariableGenerator());

        System.out.println("FlinkSQLGeneratorImpl.generateSourceQuery");
        NativeNode nativeNode = generateNativeNode(normalizedSubTree, parsedCQ);

        UnaryIQTree newTree = iqFactory.createUnaryIQTree(split.getPostProcessingConstructionNode(), nativeNode);

        return iqFactory.createIQ(query.getProjectionAtom(), newTree);
    }

    /**
     * TODO: what about the distinct?
     * TODO: move the distinct and slice lifting to the post-processing splitter
     */
    private IQTree normalizeSubTree(IQTree subTree, VariableGenerator variableGenerator) {

        IQTree sliceLiftedTree = liftSlice(subTree);
        if (IS_DEBUG_ENABLED)
            log.debug("New query after lifting the slice: \n" + sliceLiftedTree);

        // TODO: check if still needed
        IQTree flattenSubTree = unionFlattener.optimize(sliceLiftedTree, variableGenerator);
        if (IS_DEBUG_ENABLED)
            log.debug("New query after flattening the union: \n" + flattenSubTree);

        IQTree pushedDownSubTree = pushDownTransformer.transform(flattenSubTree);
        if (IS_DEBUG_ENABLED)
            log.debug("New query after pushing down: \n" + pushedDownSubTree);

        IQTree treeAfterPullOut = optimizerFactory.createEETransformer(variableGenerator).transform(pushedDownSubTree);
        if (IS_DEBUG_ENABLED)
            log.debug("Query tree after pulling out equalities: \n" + treeAfterPullOut);

        // Dialect specific
        IQTree afterDialectNormalization = extraNormalizer.transform(treeAfterPullOut, variableGenerator);
        if (IS_DEBUG_ENABLED)
            log.debug("New query after the dialect-specific extra normalization: \n" + afterDialectNormalization);
        return afterDialectNormalization;
    }

    private IQTree liftSlice(IQTree subTree) {
        if (subTree.getRootNode() instanceof ConstructionNode) {
            ConstructionNode constructionNode = (ConstructionNode) subTree.getRootNode();
            IQTree childTree = ((UnaryIQTree) subTree).getChild();
            if (childTree.getRootNode() instanceof SliceNode) {
                /*
                 * Swap the top construction node and the slice
                 */
                SliceNode sliceNode = (SliceNode) childTree.getRootNode();
                IQTree grandChildTree = ((UnaryIQTree) childTree).getChild();

                return iqFactory.createUnaryIQTree(sliceNode,
                        iqFactory.createUnaryIQTree(constructionNode, grandChildTree));
            }
        }
        return subTree;
    }

    private NativeNode generateNativeNode(IQTree normalizedSubTree, ContinuousQuery parsedCQ) {
        System.out.println("FlinkSQLGeneratorImpl.generateNativeNode");
        return RSPIQTree2NativeNodeGenerator.generate(normalizedSubTree, dbParameters, parsedCQ, false);
    }
}
