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
 *   Jan 23, 2018 (wiswedel): created
 */
package org.knime.core.node.workflow;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.knime.core.data.IDataRepository;
import org.knime.core.data.container.ContainerTable;
import org.knime.core.data.filestore.internal.IFileStoreHandler;
import org.knime.core.data.filestore.internal.IWriteFileStoreHandler;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.util.CheckUtils;

/**
 * A repository of all the data elements (tables and file stores) that are held in a workflow. Used to enable
 * dereferencing blobs, filestores and linked tables. Also handles the handing out of {@link BufferedDataTable}'s
 * internal identifiers.
 *
 * @noreference This class is not intended to be referenced by clients.
 * @author Bernd Wiswedel, KNIME AG, Zurich, Switzerland
 */
public class WorkflowDataRepository implements IDataRepository {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(WorkflowDataRepository.class);

    /** Instance used for old workflows where KNIME didn't have blobs and such. */
    public static final WorkflowDataRepository OLD_WORKFLOWS_INSTANCE = new Version1xWorkflowDataRepository();

    /**
     * Maps buffer ID (as {@link org.knime.core.data.container.Buffer#getBufferID()} to their tables. Tables are output
     * tables or node internal held tables.
     */
    private final Map<Integer, ContainerTable> m_globalTableRepository;

    private final ConcurrentHashMap<UUID, IWriteFileStoreHandler> m_handlerMap;

    /**
     * internal ID for any generated buffered data table.
     */
    private final AtomicInteger LAST_ID = new AtomicInteger(0);

    WorkflowDataRepository() {
        // synchronized as per bug 3383: workflow manager's table repository must synchronized
        // (problems with GroupLoop start "forgetting" its sorted table)
        m_globalTableRepository = Collections.synchronizedMap(new HashMap<Integer, ContainerTable>());
        m_handlerMap = new ConcurrentHashMap<UUID, IWriteFileStoreHandler>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int generateNewID() {
        // see AP-10195: we accept a buffer overflow: 2147483647 -> -2147483648(neg)-> -2147483647 -> ... -> -2
        int id = LAST_ID.incrementAndGet();
        CheckUtils.checkArgument(id != -1, "Range of table IDs exhausted (integer max)");
        return id;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getLastId() {
        return LAST_ID.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateLastId(final int id) {
        // table ID -1 is used for old workflows (1.1.x) - no notion of blobs at that time
        // see also DataContainer.readFromZip(ReferencedFile, BufferCreator)
        LAST_ID.updateAndGet(
            lastId -> (id != -1 && Integer.toUnsignedLong(id) > Integer.toUnsignedLong(lastId)) ? id : lastId);
    }

    @Override
    public void addTable(final int key, final ContainerTable table) {
        m_globalTableRepository.put(key, CheckUtils.checkArgumentNotNull(table));
    }

    @Override
    public Optional<ContainerTable> getTable(final int key) {
        return Optional.ofNullable(m_globalTableRepository.get(key));
    }

    @Override
    public Optional<ContainerTable> removeTable(final Integer key) {
        return Optional.ofNullable(m_globalTableRepository.remove(key));
    }

    /** Used in test case.
     * @return the globalTableRepository
     */
    Map<Integer, ContainerTable> getGlobalTableRepository() {
        return m_globalTableRepository;
    }

    @Override
    public void addFileStoreHandler(final IWriteFileStoreHandler handler) {
        final UUID storeUUID = handler.getStoreUUID();
        if (storeUUID != null) {
            m_handlerMap.put(storeUUID, handler);
            LOGGER.debug("Adding handler " + handler + " - " + m_handlerMap.size() + " in total");
        }
    }

    @Override
    public void removeFileStoreHandler(final IWriteFileStoreHandler handler) {
        final UUID storeUUID = handler.getStoreUUID();
        if (storeUUID != null) {
            IFileStoreHandler old = m_handlerMap.remove(storeUUID);
            if (old == null) {
                throw new IllegalArgumentException("No such file store hander: " + handler);
            }
            LOGGER.debug("Removing handler " + handler + " - " + m_handlerMap.size() + " remaining");
        }
    }

    @Override
    public IFileStoreHandler getHandler(final UUID storeHandlerUUID) {
        return m_handlerMap.get(storeHandlerUUID);
    }

    @Override
    public IFileStoreHandler getHandlerNotNull(final UUID storeHandlerUUID) {
        IFileStoreHandler h = m_handlerMap.get(storeHandlerUUID);
        if (h == null) {
            final String s = "Unknown file store handler to UUID " + storeHandlerUUID;
            LOGGER.error(s);
            printValidFileStoreHandlersToLogDebug();
            throw new IllegalStateException(s);
        }
        return h;
    }

    @Override
    public void printValidFileStoreHandlersToLogDebug() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Valid file store handlers are:");
            LOGGER.debug("--------- Start --------------");
            for (IFileStoreHandler fsh : m_handlerMap.values()) {
                LOGGER.debug("  " + fsh);
            }
            LOGGER.debug("--------- End ----------------");
        }
    }

    public Collection<IWriteFileStoreHandler> getWriteFileStoreHandlers() {
        return m_handlerMap.values();
    }

    /** Restores data files into temp folder (if not done so before). */
    void ensureOpenAfterLoad() {
        synchronized (m_globalTableRepository) {
            m_globalTableRepository.values().stream().forEach(ContainerTable::ensureOpen);
        }
        for (IWriteFileStoreHandler writeFileStoreHandler : getWriteFileStoreHandlers()) {
            try {
                writeFileStoreHandler.ensureOpenAfterLoad();
            } catch (IOException e) {
                LOGGER.error("Could not open file store handler " + writeFileStoreHandler, e);
            }
        }

    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return String.format("%d table(s), %d file store handler(s)", m_globalTableRepository.size(),
            m_handlerMap.size());
    }

    private static final class Version1xWorkflowDataRepository extends WorkflowDataRepository {
        @Override
        public void addTable(final int key, final ContainerTable table) {
            throw new UnsupportedOperationException("not to be called");
        }
        @Override
        public Optional<ContainerTable> removeTable(final Integer key) {
            throw new UnsupportedOperationException("not to be called");
        }
        @Override
        public void addFileStoreHandler(final IWriteFileStoreHandler handler) {
            throw new UnsupportedOperationException("not to be called");
        }
        @Override
        public void removeFileStoreHandler(final IWriteFileStoreHandler handler) {
            throw new UnsupportedOperationException("not to be called");
        }
    }

}
