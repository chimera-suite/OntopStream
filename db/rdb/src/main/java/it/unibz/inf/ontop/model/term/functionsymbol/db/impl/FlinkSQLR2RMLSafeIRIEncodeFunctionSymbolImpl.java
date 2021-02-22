package it.unibz.inf.ontop.model.term.functionsymbol.db.impl;

import it.unibz.inf.ontop.model.type.DBTermType;

public class FlinkSQLR2RMLSafeIRIEncodeFunctionSymbolImpl extends DefaultSQLR2RMLSafeIRIEncodeFunctionSymbol {

    protected FlinkSQLR2RMLSafeIRIEncodeFunctionSymbolImpl(DBTermType dbStringType) {
        super(dbStringType);
    }

    @Override
    protected String encodeSQLStringConstant(String constant) {
        return super.encodeSQLStringConstant(constant.replace("\\", "\\\\"));
    }
}
