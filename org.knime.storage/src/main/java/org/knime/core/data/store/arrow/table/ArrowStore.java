
package org.knime.core.data.store.arrow.table;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.ValueVector;
import org.knime.core.data.store.arrow.table.column.ArrowWritableDoubleColumn;
import org.knime.core.data.store.arrow.table.value.ArrowReadableDoubleColumn;
import org.knime.core.data.store.table.column.ColumnSchema;
import org.knime.core.data.store.table.column.ReadableColumn;
import org.knime.core.data.store.table.column.WritableColumn;

public class ArrowStore implements Store {

	// numValues corresponds to numSchemata * BATCH_SIZE. not bytes!
	// TODO maybe adaptive later?
	private static final int BATCH_SIZE = 1024;

	private final ColumnSchema[] m_schemas;

	private final VectorStore<?>[] m_vectorStores;

	private final WritableColumn[] m_writableLogicalColumns;

	private final ReadableColumn[] m_readableLogicalColumns;

	private final ValueVector[] m_columnVectors;

	private final RootAllocator m_rootAllocator;

	// TODO Schema should be implemented differently in case of very large tables
	// (
	// > INT.MAX_VALUE)
	// TODO e.g. different access on schemas & different column creation
	public ArrowStore(final ColumnSchema[] schemas) {
		m_schemas = schemas;

		// TODO likely we want to have a child-root allocator, passed from some
		// central
		// mem-management
		m_rootAllocator = new RootAllocator();

		m_vectorStores = new VectorStore<?>[m_schemas.length];
		m_writableLogicalColumns = new WritableColumn[m_schemas.length];
		m_readableLogicalColumns = new ReadableColumn[m_schemas.length];

		// TODO: Factory
		for (int i = 0; i < m_schemas.length; i++) {
			switch (m_schemas[i].getType()) {
				case DOUBLE: {
					final VectorStore<Float8Vector> vectorStore = new CachedVectorStore<Float8Vector>() {

						@Override
						protected Float8Vector createNewChunk() {
							final Float8Vector vector = new Float8Vector("TODO", m_rootAllocator);
							vector.allocateNew(BATCH_SIZE);
							return vector;
						}
					};
					m_vectorStores[i] = vectorStore;
					m_writableLogicalColumns[i] = new ArrowWritableDoubleColumn(vectorStore);
					m_readableLogicalColumns[i] = new ArrowReadableDoubleColumn(vectorStore);
				}
				case STRING:
				default:
			}
		}
	}

	@Override
	public long getNumLogicalColumns() {
		return m_schemas.length;
	}

	@Override
	public WritableColumn getWritableLogicalColumnAt(final long index) {
		return m_writableLogicalColumns[(int) index];
	}

	@Override
	public ReadableColumn getReadableLogicalColumnAt(final long index) {
		return m_readableLogicalColumns[(int) index];
	}
}
