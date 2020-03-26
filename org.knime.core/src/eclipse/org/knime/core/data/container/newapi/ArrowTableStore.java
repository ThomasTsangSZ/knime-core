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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.container.newapi.readers.BooleanArrowReaderFactory;
import org.knime.core.data.container.newapi.writers.BooleanArrowWriterFactory;
import org.knime.core.data.container.newapi.writers.DoubleArrowWriterFactory;
import org.knime.core.data.container.newapi.writers.StringArrowWriterFactory;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.util.FileUtil;

/**
 *
 * @author dietzc
 */
public class ArrowTableStore implements TableStore {

    // TODO Logging
    private static final Map<DataType, ArrowWriterFactory> WRITER_FACTORIES = new HashMap<>();

    private static final Map<DataType, ArrowReaderFactory<?>> READER_FACTORIES = new HashMap<>();

    // TODO configurable?
    // TODO memory dependent?
    // TODO ?
    private static final int BATCH_SIZE = 64;

    {
        // TODO let's make this extensible later on..
        // TODO missing collections
        WRITER_FACTORIES.put(StringCell.TYPE, new StringArrowWriterFactory());
        WRITER_FACTORIES.put(DoubleCell.TYPE, new DoubleArrowWriterFactory());
        WRITER_FACTORIES.put(BooleanCell.TYPE, new BooleanArrowWriterFactory());

        // TODO more types
        READER_FACTORIES.put(BooleanCell.TYPE, new BooleanArrowReaderFactory());
    }

    private RootAllocator m_rootAllocator;

    private int m_ctr;

    private File m_destFile;

    private DataTableSpec m_spec;

    private boolean m_isWriting = false;;

    // TODO maybe later we want to get rid of File for something more generic, e.g. URI or "Connection" or ...
    // TODO we likely don't need the entire spec here...; actually don't need KNIME-specific spec at all since we're only dealing with standard Java types
    // TODO we want to make sure to "chunk data" later. Dest File could be a directory...
    // TODO later we can add serializers of all sorts to the table..
    // TODO we also want to be able to identify consecutive primitive types of same type in the table which are then stored as an array in arrow, rather than individual columns (later).
    public ArrowTableStore(final DataTableSpec spec) {
        m_spec = spec;
        m_rootAllocator = new RootAllocator(Long.MAX_VALUE);
        try {
            // TODO later... directory :-)
            m_destFile = FileUtil.createTempFile(UUID.randomUUID().toString(), ".arrow");
        } catch (IOException ex) {
            // TODO handle later!!!
            // TODO @SimonSchmid :-))
        }
    }

    @Override
    public TableStoreWriteAccess createWriteAccess() {
        // yey, not synchronized. only tmp anyways.
        if (m_isWriting) {
            throw new IllegalStateException("only single writer supported");
        }
        m_isWriting = true;
        return new ArrowTableStoreWriteAccess();
    }

    @Override
    public TableStoreReadAccess createReadAccess(final TableStoreAccessConfig config) {
        if (m_isWriting) {
            // TODO implement synchrouniosuosuosu read/write
            throw new IllegalStateException("Not allowed atm");
        }
        return new ArrowTableStoreReadAccess();
    }

    @Override
    public void destroy() {
        m_rootAllocator.close();
        m_destFile.delete();
    }

    // TODO: Only supports single file (integer row count) for now

    private final class ArrowTableStoreWriteAccess implements TableStoreWriteAccess {

        private final ArrowWriter[] m_writers;

        private final ArrowStreamWriter m_streamWriter;

        private long m_rowIndex = -1;

        public ArrowTableStoreWriteAccess() {
            m_writers = new ArrowWriter[m_spec.getNumColumns()];
            final List<FieldVector> vecs = new ArrayList<>();
            for (int i = 0; i < m_writers.length; i++) {
                // TODO set batch size to 1024. No idea how to set optimal number here...
                // for performance reasons we might add API for the developer (or the framework) to allocate size.
                m_writers[i] = WRITER_FACTORIES.get(m_spec.getColumnSpec(i).getType()).create(m_spec.getName(),
                    m_rootAllocator, /* TODO*/ BATCH_SIZE);
                final FieldVector vec = m_writers[i].retrieveVector(); // Closed via inserters. (TODO that feels very wrong...)
                vecs.add(vec);
            }
            try {
                RandomAccessFile raf = new RandomAccessFile(m_destFile, "rw");
                FileChannel channel = raf.getChannel();
                m_streamWriter = new ArrowStreamWriter(new VectorSchemaRoot(vecs), null, channel);
            } catch (Exception e) {
                // TODO
                throw new RuntimeException(e);
            }
        }

        @Override
        public void add(final DataRow row) {
            // TODO missing: special handling for RowKeys & Co
            // TODO missing: special handling for filestores & blobstores
            for (int i = 0; i < row.getNumCells(); i++) {
                m_writers[i].accept(row.getCell(0));
            }
            m_ctr++;
            flushIfRequired();
        }

        private void flushIfRequired() {
            if (m_ctr == BATCH_SIZE) {
                flush();
            }
        }

        private void flush() {
            try {
                m_arrowBatchWriter.writeBatch();
            } catch (Exception e) {
                // TODO later
                throw new RuntimeException(e);
            }
            m_ctr = 0;
        }

        @Override
        public long getCapacity() {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public void forward() {
            // TODO Auto-generated method stub

        }

        @Override
        public long getNumColumns() {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public void setInt(final long index, final int value) {
            m_writers[index].setIntValue(m_rowIndex, value);
        }

        @Override
        public void setString(final long index, final String value) {
            m_writers[index].setStringValue(m_rowIndex, value);
        }

        @Override
        public void close() throws Exception {
            flush();
            for (int i = 0; i < m_writers.length; i++) {
                try {
                    m_writers[i].close();
                } catch (Exception ex) {
                    // TODO we need to carefully handle exceptions (later)
                    throw new RuntimeException(ex);
                }
            }
            m_arrowBatchWriter.close();
            m_isWriting = false;
        }
    }

    // TODO: Only supports single file (integer row count) for now
    private final class ArrowTableStoreReadAccess implements TableStoreReadAccess {

        private final VectorSchemaRoot m_schemaRoot;

        private final ArrowStreamReader m_streamReader;

        private final ArrowReader[] m_readers;

        private long m_rowIndex = -1;

        public ArrowTableStoreReadAccess() {
            final RandomAccessFile raFile;
            try {
                raFile = new RandomAccessFile(m_destFile, "r");
            } catch (FileNotFoundException ex) {
                ex.printStackTrace();
            }
            m_streamReader = new ArrowStreamReader(raFile.getChannel(), m_rootAllocator);
            m_schemaRoot = m_streamReader.getVectorSchemaRoot();
            m_readers = new ArrowReader[m_spec.getNumColumns()];
            for (int i = 0; i < m_readers.length; i++) {
                final FieldVector vector = m_schemaRoot.getVector(i);
                // TODO: Make as type-safe as possible
                final ArrowReaderFactory readerFactory = READER_FACTORIES.get(m_spec.getColumnSpec(i).getType());
                m_readers[i] = readerFactory.create(vector);
            }
        }

        @Override
        public long getNumRows() {
            return m_schemaRoot.getRowCount();
        }

        @Override
        public boolean canForward() {
            return m_rowIndex < getNumRows() - 1;
        }

        @Override
        public void forward() {
            m_rowIndex++;
        }

        @Override
        public long getNumColumns() {
            return m_readers.length;
        }

        // TODO: Get rid of element-wise casts?

        @Override
        public int getInt(final long index) {
            return ((ArrowIntReader)m_readers[index]).getInt(m_rowIndex);
        }

        @Override
        public String getString(final long index) {
            return ((ArrowStringReader)m_readers[index]).getString(m_rowIndex);
        }

        @Override
        public void close() throws Exception {
            for (int i = 0; i < m_readers.length; i++) {
                try {
                    m_readers[i].close();
                } catch (Exception ex) {
                    // TODO we need to carefully handle exceptions (later)
                    throw new RuntimeException(ex);
                }
            }
            m_streamReader.close();
        }
    }
}
