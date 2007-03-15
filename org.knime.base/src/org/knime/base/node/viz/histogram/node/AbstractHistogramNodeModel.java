/*
 * -------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright, 2003 - 2007
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
 * -------------------------------------------------------------------
 * 
 * History
 *    13.03.2007 (Tobias Koetter): created
 */

package org.knime.base.node.viz.histogram.node;

import java.awt.Color;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.knime.base.node.viz.histogram.AbstractHistogramPlotter;
import org.knime.base.node.viz.histogram.datamodel.AbstractHistogramVizModel;
import org.knime.base.node.viz.histogram.datamodel.ColorColumn;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTable;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.StringValue;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.util.ColumnFilter;

/**
 * 
 * @author Tobias Koetter, University of Konstanz
 */
public abstract class AbstractHistogramNodeModel extends NodeModel {
    private static final NodeLogger LOGGER = NodeLogger
        .getLogger(AbstractHistogramNodeModel.class);
    /**Default number of rows to use.*/
    protected static final int DEFAULT_NO_OF_ROWS = 2500;
    /**Settings name for the take all rows select box.*/
    protected static final String CFGKEY_ALL_ROWS = "allRows";
    /**Settings name of the number of rows.*/
    protected static final String CFGKEY_NO_OF_ROWS = "noOfRows";
    /**Used to store the attribute column name in the settings.*/
    protected static final String CFGKEY_X_COLNAME = "HistogramXColName";
    /**Settings name of the aggregation column name.*/
    protected static final String CFGKEY_AGGR_COLNAME = "aggrColumn";

    private DataTableSpec m_tableSpec;
    private DataColumnSpec m_xColSpec;
    private int m_xColIdx;
    private Collection<ColorColumn> m_aggrCols;
    
    private final SettingsModelInteger m_noOfRows = new SettingsModelInteger(
                CFGKEY_NO_OF_ROWS, DEFAULT_NO_OF_ROWS);
    private final SettingsModelBoolean m_allRows = new SettingsModelBoolean(
                CFGKEY_ALL_ROWS, false);
    /** The name of the x column. */
    private final SettingsModelString m_xColName = new SettingsModelString(
                CFGKEY_X_COLNAME, "");
    private SettingsModelString m_aggrColName = new SettingsModelString(
                FixedColumnHistogramNodeModel.CFGKEY_AGGR_COLNAME, "");

    /**Constructor for class AbstractHistogramNodeModel.
     * 
     * @param nrDataIns Number of data inputs.
     * @param nrDataOuts Number of data outputs.
     */
    public AbstractHistogramNodeModel(final int nrDataIns, 
            final int nrDataOuts) {
        super(nrDataIns, nrDataOuts);
        m_noOfRows.setEnabled(!m_allRows.getBooleanValue());
    }

    /**
     * @see org.knime.core.node.NodeModel #validateSettings(NodeSettingsRO)
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) 
    throws InvalidSettingsException {
        final SettingsModelBoolean model = 
            m_allRows.createCloneWithValidatedValue(settings);
        if (!model.getBooleanValue()) {
            //read the spinner value only if the user hasn't selected to 
            //retrieve all values
            m_noOfRows.validateSettings(settings);
        }
        m_xColName.validateSettings(settings);
        m_aggrColName.validateSettings(settings);
    }

    /**
     * @see org.knime.core.node.NodeModel
     *      #loadValidatedSettingsFrom(NodeSettingsRO)
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) 
    throws InvalidSettingsException {
        try {
            m_allRows.loadSettingsFrom(settings);
            m_noOfRows.loadSettingsFrom(settings);
        } catch (Exception e) {
            // In case of older nodes the row number is not available
        }
        m_xColName.loadSettingsFrom(settings);
        m_aggrColName.loadSettingsFrom(settings);
    }

    /**
     * @see org.knime.core.node.NodeModel #saveSettingsTo(NodeSettingsWO)
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_allRows.saveSettingsTo(settings);
        m_noOfRows.saveSettingsTo(settings);
        m_xColName.saveSettingsTo(settings);
        m_aggrColName.saveSettingsTo(settings);
    }

    /**
     * @see org.knime.core.node.NodeModel#loadInternals(java.io.File,
     *      org.knime.core.node.ExecutionMonitor)
     */
    @Override
    protected abstract void loadInternals(final File nodeInternDir, 
            final ExecutionMonitor exec);

    /**
     * @see org.knime.core.node.NodeModel#saveInternals( java.io.File,
     *      org.knime.core.node.ExecutionMonitor)
     */
    @Override
    protected abstract void saveInternals(final File nodeInternDir, 
            final ExecutionMonitor exec);
    /**
     * @see org.knime.core.node.NodeModel#reset()
     */
    @Override
    protected void reset() {
        m_tableSpec = null;
        m_xColSpec = null;
        m_xColIdx = -1;
        m_aggrCols = null;
    }

    /**
     * This method creates a new {@link AbstractHistogramVizModel} each time
     * it is called.
     * 
     * @return the histogram viz model or <code>null</code> if not 
     * all information are available yet
     */
    protected abstract AbstractHistogramVizModel getHistogramVizModel();

    /**
     * @see org.knime.core.node.NodeModel
     *      #configure(org.knime.core.data.DataTableSpec[])
     */
    @Override
    protected DataTableSpec[] configure(final DataTableSpec[] inSpecs) 
    throws InvalidSettingsException {
        if (inSpecs == null || inSpecs[0] == null) {
            throw new InvalidSettingsException(
                    "No input specification available.");
        }
        m_tableSpec = inSpecs[0];
        if (m_tableSpec == null || m_tableSpec.getNumColumns() < 1) {
            throw new InvalidSettingsException(
                    "Input table should have at least 1 column.");
        }

        final String xCol = m_xColName.getStringValue();
        m_xColSpec = m_tableSpec.getColumnSpec(xCol);
        if (!m_tableSpec.containsName(xCol)) {
            // if the input table has only two columns where only one column
            // is numerical select these two columns as default columns
            // if both are numeric we don't know which one the user wants as
            // aggregation column and which one as x column
            final ColumnFilter xFilter = 
                AbstractHistogramPlotter.X_COLUMN_FILTER;
            final ColumnFilter aggrFilter = 
                AbstractHistogramPlotter.AGGREGATION_COLUMN_FILTER;
            if (m_tableSpec.getNumColumns() == 1) {
                final DataColumnSpec columnSpec0 = m_tableSpec.getColumnSpec(0);
                if (xFilter.includeColumn(columnSpec0)) {
                    m_xColName.setStringValue(columnSpec0.getName());
                } else {
                    throw new InvalidSettingsException(
                        "No column compatible with this node. Column needs to "
                        + "be nominal or numeric and must contain a valid "
                        + "domain. In order to compute the domain of a column "
                        + "use the DomainCalculator or ColumnFilter node.");
                }
            } else if (m_tableSpec.getNumColumns() == 2) {
                final DataColumnSpec columnSpec0 = m_tableSpec.getColumnSpec(0);
                final DataColumnSpec columnSpec1 = m_tableSpec.getColumnSpec(1);
                final DataType type0 = columnSpec0.getType();
                final DataType type1 = columnSpec1.getType();

                if (type0.isCompatible(StringValue.class)
                        && type1.isCompatible(DoubleValue.class)
                        && xFilter.includeColumn(columnSpec0)
                        && aggrFilter.includeColumn(columnSpec1)) {
                    m_xColName.setStringValue(m_tableSpec.getColumnSpec(0)
                            .getName());
                    m_aggrColName.setStringValue(m_tableSpec.getColumnSpec(1)
                            .getName());
                } else if (type0.isCompatible(DoubleValue.class)
                        && type1.isCompatible(StringValue.class)
                        && xFilter.includeColumn(columnSpec1)
                        && aggrFilter.includeColumn(columnSpec0)) {
                    m_xColName.setStringValue(m_tableSpec.getColumnSpec(1)
                            .getName());
                    m_aggrColName.setStringValue(m_tableSpec.getColumnSpec(0)
                            .getName());
                } else {
                    throw new InvalidSettingsException(
                            "Please define the x column name.");
                }
            } else {
                throw new InvalidSettingsException(
                        "Please define the x column name.");
            }
        }
        return new DataTableSpec[0];
    }
    
    /**
     * @see org.knime.core.node.NodeModel #execute(BufferedDataTable[],
     *      ExecutionContext)
     */
    @Override
    protected BufferedDataTable[] execute(final BufferedDataTable[] inData,
            final ExecutionContext exec) throws Exception {
        LOGGER.debug("Entering execute(inData, exec) of class "
                + "FixedColumnHistogramNodeModel.");
        if (inData == null || inData[0] == null) {
            throw new Exception("No data table available!");
        }
        // create the data object
        final BufferedDataTable table = inData[0];
        final DataTableSpec tableSpec = inData[0].getDataTableSpec();
        if (tableSpec == null) {
            throw new NullPointerException(
                    "Table specification must not be null");
        }
        final int maxNoOfRows = table.getRowCount();
        if (maxNoOfRows < 0) {
            throw new IllegalArgumentException(
                    "Maximum number of rows must be a positive integer");
        }
        m_tableSpec = tableSpec;
        final String xCol = m_xColName.getStringValue();
        m_xColSpec = m_tableSpec.getColumnSpec(xCol);
        if (m_xColSpec == null) {
            throw new IllegalArgumentException("X column not found");
        }
        m_xColIdx = m_tableSpec.findColumnIndex(xCol);
        if (m_xColIdx < 0) {
            throw new IllegalArgumentException("X column index not found");
        }
        final String aggrColName = m_aggrColName.getStringValue();
        if (aggrColName == null || aggrColName.trim().length() < 1) {
            //the user hasn't selected an aggregation column
            //thats fine since it is optional
            m_aggrCols = null;
        } else {
            final int aggrColIdx = m_tableSpec.findColumnIndex(aggrColName);
            if (aggrColIdx < 0) {
                throw new IllegalArgumentException(
                        "Selected aggregation column not found.");
            }
            final ColorColumn aggrColumn = 
                new ColorColumn(Color.LIGHT_GRAY, aggrColIdx, aggrColName);
            m_aggrCols = new ArrayList<ColorColumn>(1);
            m_aggrCols.add(aggrColumn);
        }
        
        if (m_allRows.getBooleanValue()) {
            //set the actual number of rows in the selected number of rows
            //object since the user wants to display all rows
            m_noOfRows.setIntValue(maxNoOfRows);
        }
        final int selectedNoOfRows = m_noOfRows.getIntValue();
        //final int noOfRows = inData[0].getRowCount();
        if ((selectedNoOfRows) < maxNoOfRows) {
            setWarningMessage("Only the first " + selectedNoOfRows + " of " 
                    + maxNoOfRows + " rows are displayed.");
        } else if (selectedNoOfRows > maxNoOfRows) {
            m_noOfRows.setIntValue(maxNoOfRows);
        }
        createHistogramModel(exec, table);
        LOGGER.debug("Exiting execute(inData, exec) of class "
                + "FixedColumnHistogramNodeModel.");
        return new BufferedDataTable[0];
    }
    
    /**
     * This method should use the given information to create the internal
     * histogram data model.
     * @param exec the {@link ExecutionContext} for progress information
     * @param table the {@link DataTable} which contains the rows
     * @throws CanceledExecutionException if the user has canceled the
     * node execution
     */
    protected abstract void createHistogramModel(final ExecutionContext exec, 
            final DataTable table) 
    throws CanceledExecutionException;
    
    /**
     * @return the {@link DataTableSpec} of the input table
     */
    protected DataTableSpec getTableSpec() {
        return m_tableSpec;
    }

    /**
     * @return the aggregation columns to use or <code>null</code> if
     * the user hasn't selected a aggregation column
     */
    protected Collection<ColorColumn> getAggrColumns() {
        return m_aggrCols;
    }
    
    /**
     * @return the name of the selected x column or null if none is selected
     */
    protected String getSelectedXColumnName() {
        final String value = m_xColName.getStringValue();
        if (value == null || value.trim().length() < 1) {
            return null;
        }
        return value;
    }
    
    /**
     * @param name the new selected x column name
     */
    protected void setSelectedXColumnName(final String name) {
        if (name == null) {
            throw new NullPointerException("Name must not be null");
        }
        m_xColName.setStringValue(name);
    }
    
    /**
     * @return all selected aggregation column names or null if none is
     * selected
     */
    protected List<String> getSelectedAggrColNames() {
        final String value = m_aggrColName.getStringValue();
        if (value == null || value.trim().length() < 1) {
            return null;
        }
        final ArrayList<String> names = new ArrayList<String>(1);
        names.add(value);
        return names;
    }
    
    /**
     * @param names the new selected aggregation names
     */
    protected void setSelectedAggrColNames(final List<String> names) {
        if (names == null || names.size() < 1) {
            throw new NullPointerException("Names must not be null or empty");
        }
        m_aggrColName.setStringValue(names.get(0));
    }
    
    /**
     * @return the {@link DataColumnSpec} of the selected x column
     */
    protected DataColumnSpec getXColSpec() {
        return m_xColSpec;
    }
    
    /**
     * @return the index of the selected x column in the given 
     * {@link DataTableSpec}
     */
    protected int getXColIdx() {
        return m_xColIdx;
    }

    /**
     * @return the number of rows to add to the histogram
     */
    protected int getNoOfRows() {
        return m_noOfRows.getIntValue();
    }
}
