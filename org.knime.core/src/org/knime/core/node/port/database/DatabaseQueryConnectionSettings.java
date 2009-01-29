/* 
 * ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright, 2003 - 2009
 * University of Konstanz, Germany
 * Chair for Bioinformatics and Information Mining (Prof. M. Berthold)
 * and KNIME GmbH, Konstanz, Germany
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.org
 * email: contact@knime.org
 * --------------------------------------------------------------------- *
 * 
 */
package org.knime.core.node.port.database;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.ModelContent;
import org.knime.core.node.ModelContentRO;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.node.config.ConfigWO;

/**
 * 
 * @author Thomas Gabriel, University of Konstanz
 */
public final class DatabaseQueryConnectionSettings 
        extends DatabaseConnectionSettings {
    
    /** Place holder <code>&lttable&gt</code>. */
    public static final String TABLE_PLACEHOLDER = "<table>";
    
    private final String m_query;
    
    /**
     * Create a new connection with an empty query object.
     * @param settings settings to load from
     * @throws InvalidSettingsException if settings could not be loaded
     */
    public DatabaseQueryConnectionSettings(final ConfigRO settings) 
            throws InvalidSettingsException {
        super();
        m_query = settings.getString(DatabaseConnectionSettings.CFG_STATEMENT);
        super.loadValidatedConnection(settings);
    }
    
    /**
     * Creates a new connection based in the given connection and the query
     * string.
     * @param conn connection to copy
     * @param query the SQL query
     */
    public DatabaseQueryConnectionSettings(
            final DatabaseConnectionSettings conn, 
            final String query) {
        super(conn);
        m_query = query;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void validateConnection(final ConfigRO settings)
            throws InvalidSettingsException {
        String query = settings.getString(CFG_STATEMENT);
        if (query != null && query.contains(TABLE_PLACEHOLDER)) {
            throw new InvalidSettingsException("Database table place holder (" 
                    + TABLE_PLACEHOLDER + ") not replaced in query:\n"
                    + query);
        }
        super.validateConnection(settings);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void saveConnection(final ConfigWO settings) {
        settings.addString(CFG_STATEMENT, m_query);
        super.saveConnection(settings);
    }
    
    /**
     * @return SQL statement 
     */
    public String getQuery() {
        return m_query;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public ModelContentRO createConnectionModel() {
        ModelContent cont = new ModelContent("database_query_connection_model");
        saveConnection(cont);
        return cont;
    }        

}
