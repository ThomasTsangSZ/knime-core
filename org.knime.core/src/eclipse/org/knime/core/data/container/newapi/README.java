/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   Mar 26, 2020 (dietzc): created
 */
package org.knime.core.data.container.newapi;

/**
 *
 * @author dietzc
 */
public interface README {
    /**
     * DESIGN REQUIREMENTS (besides interface-driven, composition over inheritence, modularity)
     *  -> Enable (extensible) shared memory for python / java
     *  -> Allow API for proxy-types
     *  -> wide-table support
     *  -> backwards-compatibility
     *  -> ultra-lightweight processing. Any processing on KNIME side should be much faster than any TableIO.
     *  -> Predicate push-down and Filter-API
     *
     *  Nice to haves for later
     *  -> Chunking (for parallel read / write of data & distributed computing "KNIMETable layer with map/reduce like operations on-top")
     *  -> Expose columnar API to end-user
     */
}


//
///**
//* Overridden to narrow return type to closeable iterator. {@inheritDoc}
//*/
//@Override
//public CloseableRowIterator iterator();
//
///**
//* Provides a {@link CloseableRowIterator} that is filtered according to a given {@link TableFilter}. The filtering
//* won't change this KnowsRowCountTable or impact subsequent calls of this method with other filters.
//*
//* @param filter the filter to be applied
//* @return a filtered iterator
//* @since 4.0
//*/
//default CloseableRowIterator iteratorWithFilter(final TableFilter filter) {
//  return iteratorWithFilter(filter, null);
//}
//
///**
//* Provides a {@link CloseableRowIterator} that is filtered according to a given {@link TableFilter}. During
//* iteration, a given {@link ExecutionMonitor} will update its progress. The filtering won't change this
//* KnowsRowCountTable or impact subsequent calls of this method with other filters.
//*
//* @param filter the filter to be applied
//* @param exec the execution monitor that shall be updated with progress or null if no progress updates are desired
//* @return a filtered iterator
//* @since 4.0
//*/
//CloseableRowIterator iteratorWithFilter(TableFilter filter, ExecutionMonitor exec);