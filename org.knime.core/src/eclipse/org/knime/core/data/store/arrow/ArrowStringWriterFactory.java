/*
 * ------------------------------------------------------------------------
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
 * ------------------------------------------------------------------------
 */

package org.knime.core.data.store.arrow;

import java.nio.charset.StandardCharsets;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.knime.core.data.store.PrimitiveRow;

public final class ArrowStringWriterFactory implements ArrowWriterFactory<VarCharVector> {

    @Override
    @SuppressWarnings("resource") // Vector will be closed by writer.
    public ArrowStringWriter create(final String name, final BufferAllocator allocator, final int numRows,
        final int colIdx) {
        final VarCharVector vector = new VarCharVector(name, allocator);
        // TODO more flexible configuration of "bytes per cell assumption". E.g. rowIds might be smaller
        vector.allocateNew(64l * numRows, numRows);
        return new ArrowStringWriter(vector, colIdx);
    }

    public static final class ArrowStringWriter extends AbstractArrowWriter<VarCharVector> {

        private int m_byteCount = 0;

        public ArrowStringWriter(final VarCharVector vector, final int colIdx) {
            super(vector, colIdx);
        }

        @Override
        protected void writeNonNull(final int index, final PrimitiveRow value, final int colIdx) {
            if (index >= m_vector.getValueCapacity()) {
                m_vector.reallocValidityAndOffsetBuffers();
            }
            final byte[] bytes = value.getString(colIdx).getBytes(StandardCharsets.UTF_8);
            m_byteCount += bytes.length;
            while (m_byteCount > m_vector.getByteCapacity()) {
                m_vector.reallocDataBuffer();
            }
            m_vector.set(index, bytes);
        }

    }
}
