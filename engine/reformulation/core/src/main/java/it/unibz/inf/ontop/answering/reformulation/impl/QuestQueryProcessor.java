package it.unibz.inf.ontop.answering.reformulation.impl;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import it.unibz.inf.ontop.answering.logging.QueryLogger;
import it.unibz.inf.ontop.answering.reformulation.QueryCache;
import it.unibz.inf.ontop.answering.reformulation.QueryReformulator;
import it.unibz.inf.ontop.answering.reformulation.generation.NativeQueryGenerator;
import it.unibz.inf.ontop.answering.reformulation.input.InputQuery;
import it.unibz.inf.ontop.answering.reformulation.input.InputQueryFactory;
import it.unibz.inf.ontop.answering.reformulation.input.translation.InputQueryTranslator;
import it.unibz.inf.ontop.answering.reformulation.rewriting.QueryRewriter;
import it.unibz.inf.ontop.answering.reformulation.unfolding.QueryUnfolder;
import it.unibz.inf.ontop.exception.OntopReformulationException;
import it.unibz.inf.ontop.injection.TranslationFactory;
import it.unibz.inf.ontop.iq.IQ;
import it.unibz.inf.ontop.iq.exception.EmptyQueryException;
import it.unibz.inf.ontop.iq.optimizer.*;
import it.unibz.inf.ontop.iq.planner.QueryPlanner;
import it.unibz.inf.ontop.iq.tools.ExecutorRegistry;
import it.unibz.inf.ontop.spec.OBDASpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

/**
 * TODO: rename it QueryTranslatorImpl ?
 *
 * See ReformulationFactory for creating a new instance.
 *
 */
public class QuestQueryProcessor implements QueryReformulator {

	private final QueryRewriter rewriter;
	private final NativeQueryGenerator datasourceQueryGenerator;
	private final QueryCache queryCache;

	private final QueryUnfolder queryUnfolder;

	private static final Logger log = LoggerFactory.getLogger(QuestQueryProcessor.class);
	private static boolean IS_DEBUG_ENABLED = log.isDebugEnabled();
	private final ExecutorRegistry executorRegistry;
	private final InputQueryTranslator inputQueryTranslator;
	private final InputQueryFactory inputQueryFactory;
	private final GeneralStructuralAndSemanticIQOptimizer generalOptimizer;
	private final QueryPlanner queryPlanner;
	private final QueryLogger.Factory queryLoggerFactory;

	@AssistedInject
	private QuestQueryProcessor(@Assisted OBDASpecification obdaSpecification,
								@Assisted ExecutorRegistry executorRegistry,
								QueryCache queryCache,
								TranslationFactory translationFactory,
								QueryRewriter queryRewriter,
								InputQueryFactory inputQueryFactory,
								InputQueryTranslator inputQueryTranslator,
								GeneralStructuralAndSemanticIQOptimizer generalOptimizer,
								QueryPlanner queryPlanner,
								QueryLogger.Factory queryLoggerFactory) {
		this.inputQueryFactory = inputQueryFactory;
		this.rewriter = queryRewriter;
		this.generalOptimizer = generalOptimizer;
		this.queryPlanner = queryPlanner;
		this.queryLoggerFactory = queryLoggerFactory;

		this.rewriter.setTBox(obdaSpecification.getSaturatedTBox());
		this.queryUnfolder = translationFactory.create(obdaSpecification.getSaturatedMapping());
		this.datasourceQueryGenerator = translationFactory.create(obdaSpecification.getDBParameters());

		this.inputQueryTranslator = inputQueryTranslator;
		this.queryCache = queryCache;
		this.executorRegistry = executorRegistry;

		log.info("Ontop has completed the setup and it is ready for query answering!");
	}

	@Override
	public IQ reformulateIntoNativeQuery(InputQuery inputQuery, QueryLogger queryLogger)
			throws OntopReformulationException {

		long beginning = System.currentTimeMillis();

		IQ cachedQuery = queryCache.get(inputQuery);
		if (cachedQuery != null) {
			queryLogger.declareReformulationFinishedAndSerialize(true);
			return cachedQuery;
		}

		try {
			if (IS_DEBUG_ENABLED)
				log.debug("SPARQL query:\n{}", inputQuery.getInputString());
			IQ convertedIQ = inputQuery.translate(inputQueryTranslator);
			log.debug("Parsed query converted into IQ (after normalization):\n{}", convertedIQ);

            try {
                log.debug("Start the rewriting process...");
                IQ rewrittenIQ = rewriter.rewrite(convertedIQ);

                if (IS_DEBUG_ENABLED)
                	log.debug("Rewritten IQ:\n{}",rewrittenIQ);

                log.debug("Start the unfolding...");

                IQ unfoldedIQ = queryUnfolder.optimize(rewrittenIQ);
                if (unfoldedIQ.getTree().isDeclaredAsEmpty()) {
                	log.debug(String.format("Reformulation time: %d ms", System.currentTimeMillis() - beginning));
					queryLogger.declareReformulationFinishedAndSerialize(false);
					return unfoldedIQ;
				}

                // These IQ can be large so getting the string can be expensive
                if (IS_DEBUG_ENABLED)
                	log.debug("Unfolded query: \n" + unfoldedIQ.toString());

                IQ optimizedQuery = generalOptimizer.optimize(unfoldedIQ, executorRegistry);
				IQ plannedQuery = queryPlanner.optimize(optimizedQuery, executorRegistry);
				if (IS_DEBUG_ENABLED)
					log.debug("Planned query: \n" + plannedQuery);

				IQ executableQuery = generateExecutableQuery(plannedQuery);
				queryCache.put(inputQuery, executableQuery);
				log.debug(String.format("Reformulation time: %d ms", System.currentTimeMillis() - beginning));
				queryLogger.declareReformulationFinishedAndSerialize(false);
				return executableQuery;

			}
            catch (OntopReformulationException e) {
                throw e;
            }
        }
		/*
		 * Bug: should normally not be reached
		 * TODO: remove it
		 */
		catch (Exception e) {
			log.warn("Unexpected exception: " + e.getMessage(), e);
			// TODO: involve the query logger
			throw new OntopReformulationException(e);
			//throw new OntopReformulationException("Error rewriting and unfolding into SQL\n" + e.getMessage());
		}
	}

	private IQ generateExecutableQuery(IQ iq) {

		log.debug("Producing the native query string...");

		IQ executableQuery = datasourceQueryGenerator.generateSourceQuery(iq);

		if (IS_DEBUG_ENABLED)
			log.debug("Resulting native query: \n{}", executableQuery);

		try {
			Path fileName = Path.of("/home/matbelcao/Scrivania/query.txt");
			String content = executableQuery.getTree().toString();
			Files.writeString(fileName, content);
		}catch(IOException e){
			System.out.println("Error on writing the debug query on a file. Please check the hardcoded filename location in " +
					"engine/reformulation/core/src/main/java/it/unibz/inf/ontop/answering/reformulation/impl/QuestQueryProcessor.java");
		}

		return executableQuery;
	}


	/**
	 * Returns the final rewriting of the given query
	 */
	@Override
	public String getRewritingRendering(InputQuery query) throws OntopReformulationException {
		if (IS_DEBUG_ENABLED)
			log.debug("SPARQL query:\n{}", query.getInputString());
		IQ convertedIQ = query.translate(inputQueryTranslator);
		if (IS_DEBUG_ENABLED)
			log.debug("Parsed query converted into IQ:\n{}", convertedIQ);
		try {
			IQ rewrittenIQ = rewriter.rewrite(convertedIQ);
			return rewrittenIQ.toString();
		}
		catch (EmptyQueryException e) {
			e.printStackTrace();
		}
		return "EMPTY REWRITING";
	}

	@Override
	public InputQueryFactory getInputQueryFactory() {
		return inputQueryFactory;
	}

	@Override
	public QueryLogger.Factory getQueryLoggerFactory() {
		return  queryLoggerFactory;
	}
}
