package it.unibz.inf.ontop.spec.mapping.transformer.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import it.unibz.inf.ontop.injection.IntermediateQueryFactory;
import it.unibz.inf.ontop.iq.IQ;
import it.unibz.inf.ontop.iq.IQTree;
import it.unibz.inf.ontop.iq.node.*;
import it.unibz.inf.ontop.model.atom.*;
import it.unibz.inf.ontop.model.term.*;
import it.unibz.inf.ontop.model.term.functionsymbol.ExpressionOperation;
import it.unibz.inf.ontop.model.vocabulary.RDF;
import it.unibz.inf.ontop.spec.mapping.utils.MappingTools;
import it.unibz.inf.ontop.substitution.ImmutableSubstitution;
import it.unibz.inf.ontop.substitution.SubstitutionFactory;
import it.unibz.inf.ontop.utils.CoreUtilsFactory;
import it.unibz.inf.ontop.utils.ImmutableCollectors;
import it.unibz.inf.ontop.utils.VariableGenerator;
import org.apache.commons.rdf.api.IRI;

import java.util.*;
import java.util.stream.Stream;

/***
 * Splits a given mapping into builtin predicates (conditions)
 * and all other atoms (stripped), which are checked for containment
 * by the TMapping construction algorithm.
 */

public class TMappingRule {

	private final MappingTools.RDFPredicateInfo predicateInfo;

	private final ImmutableList<ImmutableTerm> headTerms;
	private final ImmutableList<ExtensionalDataNode> databaseAtoms;
	// an OR-connected list of AND-connected atomic filters
	private final ImmutableList<ImmutableList<ImmutableExpression>> filterAtoms;

	private final TermFactory termFactory;
	private final AtomFactory atomFactory;
	private final IntermediateQueryFactory iqFactory;
	private final SubstitutionFactory substitutionFactory;

	/***
	 * Given a mappings in currentMapping, this method will
	 * return a new mappings in which no constants appear in the body of
	 * database predicates. This is done by replacing the constant occurrence
	 * with a fresh variable, and adding a new equality condition to the body of
	 * the mapping.
	 * <p/>
	 * 
	 * For example, let the mapping m be
	 * <p/>
	 * A(x) :- T(x,y,22)
	 * 
	 * <p>
	 * Then this method will replace m by the mapping m'
	 * <p>
	 * A(x) :- T(x,y,z), EQ(z,22)
	 * 
	 */
	
	public TMappingRule(IQ iq, TermFactory termFactory, AtomFactory atomFactory, IntermediateQueryFactory iqFactory, SubstitutionFactory substitutionFactory) {

        this.termFactory = termFactory;
        this.atomFactory = atomFactory;
        this.iqFactory = iqFactory;

		this.predicateInfo = MappingTools.extractRDFPredicate(iq);
		this.substitutionFactory = substitutionFactory;

		VariableGenerator variableGenerator = iq.getVariableGenerator();

		DistinctVariableOnlyDataAtom projectionAtom = iq.getProjectionAtom();
		ConstructionNode cn = (ConstructionNode)iq.getTree().getRootNode();
		ImmutableSubstitution sub = cn.getSubstitution();

		ImmutableSet<ImmutableExpression> joinConditions;
		ImmutableList<ExtensionalDataNode> dataAtoms;

		IQTree tree = iq.getTree().getChildren().get(0);
		if (tree.getRootNode() instanceof FilterNode) {
			joinConditions = ((FilterNode)tree.getRootNode()).getOptionalFilterCondition().get().flattenAND();
			dataAtoms = ImmutableList.of((ExtensionalDataNode)tree.getChildren().get(0));
		}
		else if (tree.getRootNode() instanceof ExtensionalDataNode) {
			joinConditions = ImmutableSet.of();
			dataAtoms = ImmutableList.of((ExtensionalDataNode)tree);
		}
		else if (tree.getRootNode() instanceof TrueNode) {
			joinConditions = ImmutableSet.of();
			dataAtoms = ImmutableList.of();
		}
		else {
			joinConditions = ((InnerJoinNode)tree.getRootNode()).getOptionalFilterCondition().map(e -> e.flattenAND()).orElseGet(ImmutableSet::of);
			dataAtoms = tree.getChildren().stream().map(n -> (ExtensionalDataNode)n).collect(ImmutableCollectors.toList());
		}

		// maps all non-constants to fresh variables
		ImmutableMap<ImmutableTerm, VariableOrGroundTerm> valueMap = dataAtoms.stream()
				.flatMap(n -> n.getProjectionAtom().getArguments().stream())
				.filter(t -> !(t instanceof Variable))
				.collect(ImmutableCollectors.toSet()).stream()
				.collect(ImmutableCollectors.toMap(t -> t, t -> variableGenerator.generateNewVariable()));

		this.databaseAtoms = dataAtoms.stream()
					.map(n -> n.newAtom(atomFactory.getDataAtom(
							n.getProjectionAtom().getPredicate(),
							n.getProjectionAtom().getArguments().stream()
									.map(term -> valueMap.getOrDefault(term, term))
									.collect(ImmutableCollectors.toList()))))
					.collect(ImmutableCollectors.toList());

		ImmutableList<ImmutableExpression> f = Stream.concat(
				joinConditions.stream(),
				valueMap.entrySet().stream()
					.map(e -> termFactory.getImmutableExpression(ExpressionOperation.EQ, e.getKey(), e.getValue())))
				.collect(ImmutableCollectors.toList());

		this.filterAtoms = f.isEmpty() ? ImmutableList.of() : ImmutableList.of(f);

		this.headTerms = sub.apply(predicateInfo.isClass()
				? ImmutableList.of(projectionAtom.getTerm(0))
				: ImmutableList.of(projectionAtom.getTerm(0), projectionAtom.getTerm(2)));
	}



	TMappingRule(TMappingRule baseRule, ImmutableList<ImmutableList<ImmutableExpression>> filterAtoms) {
        this.termFactory = baseRule.termFactory;
        this.atomFactory = baseRule.atomFactory;
        this.iqFactory = baseRule.iqFactory;
        this.substitutionFactory = baseRule.substitutionFactory;

        this.predicateInfo = baseRule.predicateInfo;

		this.databaseAtoms = baseRule.databaseAtoms;
		this.headTerms = baseRule.headTerms;

		this.filterAtoms = filterAtoms;
	}
	
	
	TMappingRule(ImmutableList<ImmutableTerm> headTerms, MappingTools.RDFPredicateInfo predicateInfo, TMappingRule baseRule) {
        this.termFactory = baseRule.termFactory;
		this.atomFactory = baseRule.atomFactory;
		this.iqFactory = baseRule.iqFactory;
		this.substitutionFactory = baseRule.substitutionFactory;

		this.predicateInfo = predicateInfo;

		this.databaseAtoms = baseRule.databaseAtoms;
		this.headTerms = headTerms;

        this.filterAtoms = baseRule.filterAtoms;
	}
	
	public IRI getIri() { return predicateInfo.getIri(); }

	public MappingTools.RDFPredicateInfo getPredicateInfo() { return predicateInfo; }


	public IQ asIQ(CoreUtilsFactory coreUtilsFactory) {

		// assumes that filterAtoms is a possibly empty list of non-empty lists
		Optional<ImmutableExpression> mergedConditions = filterAtoms.stream()
				.map(list -> list.stream()
						.reduce((r, e) -> termFactory.getImmutableExpression(ExpressionOperation.AND, e, r)).get())
				.reduce((r, e) -> termFactory.getImmutableExpression(ExpressionOperation.OR, e, r));

		IQTree tree;
		switch (databaseAtoms.size()) {
			case 0:
				tree = iqFactory.createTrueNode();
				break;
			case 1:
				tree = (mergedConditions.isPresent()
						? iqFactory.createUnaryIQTree(
								iqFactory.createFilterNode(mergedConditions.get()),
								databaseAtoms.get(0))
						: databaseAtoms.get(0));
				break;
			default:
				tree = iqFactory.createNaryIQTree(
						iqFactory.createInnerJoinNode(mergedConditions),
						databaseAtoms.stream().collect(ImmutableCollectors.toList()));
		}

		VariableGenerator generator = coreUtilsFactory.createVariableGenerator(tree.getKnownVariables());

		Variable s = generator.generateNewVariable();
		Variable p = generator.generateNewVariable();
		Variable o = generator.generateNewVariable();

		ImmutableSubstitution sub;
		if (predicateInfo.isClass()) {
			sub = substitutionFactory.getSubstitution(
					s, headTerms.get(0),
					p, getConstantIRI(RDF.TYPE),
					o, getConstantIRI(predicateInfo.getIri()));
		}
		else {
			if (headTerms.get(1) instanceof Variable) {
				o = (Variable)headTerms.get(1);
				sub = substitutionFactory.getSubstitution(
						s, headTerms.get(0),
						p, getConstantIRI(predicateInfo.getIri()));
			}
			else
				sub = substitutionFactory.getSubstitution(
					s, headTerms.get(0),
					p, getConstantIRI(predicateInfo.getIri()),
					o, headTerms.get(1));
		}

		return iqFactory.createIQ(
				atomFactory.getDistinctTripleAtom(s, p, o),
				iqFactory.createUnaryIQTree(
						iqFactory.createConstructionNode(ImmutableSet.of(s, p, o), sub),
						tree));
	}

	private ImmutableFunctionalTerm getConstantIRI(IRI iri) {
		return termFactory.getImmutableUriTemplate(termFactory.getConstantLiteral(iri.getIRIString()));
	}

	public ImmutableList<ImmutableTerm> getHeadTerms() {
        return headTerms;
    }

	public ImmutableList<ExtensionalDataNode> getDatabaseAtoms() {
		return databaseAtoms;
	}

	public ImmutableList<ImmutableList<ImmutableExpression>> getConditions() { return filterAtoms; }
	
	@Override
	public int hashCode() {
		return predicateInfo.getIri().hashCode() ^ headTerms.hashCode() ^ databaseAtoms.hashCode() ^ filterAtoms.hashCode();
	}
	
	@Override
	public boolean equals(Object other) {
		if (other instanceof TMappingRule) {
			TMappingRule otherRule = (TMappingRule)other;
			return (headTerms.equals(otherRule.headTerms) &&
					databaseAtoms.equals(otherRule.databaseAtoms) && 
					filterAtoms.equals(otherRule.filterAtoms));
		}
		return false;
	}

	@Override 
	public String toString() {
		return predicateInfo.getIri() + "(" + headTerms + ") <- " + databaseAtoms + " AND " + filterAtoms;
	}
}
