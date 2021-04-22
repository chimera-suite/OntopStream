package it.unibz.inf.ontop.generation.algebra.impl;

import com.google.common.collect.ImmutableList;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import it.unibz.inf.ontop.generation.algebra.SQLExpression;
import it.unibz.inf.ontop.generation.algebra.SQLNaryJoinExpression;
import it.unibz.inf.ontop.generation.algebra.SQLRelationVisitor;
import it.unibz.inf.ontop.generation.algebra.SQLTable;
import it.unibz.inf.ontop.model.term.ImmutableExpression;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class FlinkSQLNaryJoinExpressionImpl implements SQLNaryJoinExpression {
    private final ImmutableList<SQLExpression> joinedExpressions;

    @AssistedInject
    private FlinkSQLNaryJoinExpressionImpl(@Assisted ImmutableList<SQLExpression> joinedExpressions) {

        List<SQLExpression> sortedJoinedExpressions = new ArrayList<SQLExpression>(joinedExpressions);

        /*
         * Change the tables order for allowing the correct execution of the LATERAL TABLE operator of FlinkSQL,
         * which is needed for joining multiple tables.
         */
        Collections.sort(sortedJoinedExpressions, (exp1, exp2) -> {
            if((exp2 instanceof SQLTable) && !(exp1 instanceof SQLTable)){
                return +1;
            }
            if(!(exp2 instanceof SQLTable) && (exp1 instanceof SQLTable)){
                return -1;
            }
            return 0;
        });

        this.joinedExpressions = ImmutableList.copyOf(sortedJoinedExpressions);
    }

    @Override
    public ImmutableList<SQLExpression> getJoinedExpressions() {
        return this.joinedExpressions;
    }

    @Override
    public Optional<ImmutableExpression> getFilterCondition() {
        return Optional.empty();
    }

    @Override
    public <T> T acceptVisitor(SQLRelationVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
