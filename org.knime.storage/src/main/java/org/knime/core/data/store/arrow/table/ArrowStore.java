package org.knime.core.data.store.arrow.table;

import java.io.File;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.knime.core.data.store.Store;
import org.knime.core.data.store.arrow.table.value.ArrowBooleanColumnPartitionStore;
import org.knime.core.data.store.arrow.table.value.ArrowDoubleColumnPartitionStore;
import org.knime.core.data.store.arrow.table.value.ArrowStringColumnPartitionStore;
import org.knime.core.data.store.partition.ColumnPartitionStore;
import org.knime.core.data.store.table.column.ColumnType;

public class ArrowStore implements Store {

	private File m_baseDirectory;
	private int m_batchSize;
	private RootAllocator m_rootAllocator;

	public ArrowStore(File baseDirectory, int batchSize, long maxAllocationInBytes) {
		m_baseDirectory = baseDirectory;
		m_batchSize = batchSize;
		m_rootAllocator = new RootAllocator(maxAllocationInBytes);
	}

	// TODO I think this cast is OK, as an ArrowStore actually provides multiple
	// different instances of ColumnPartitionStores with different types.
	@Override
	public ColumnPartitionStore<?> create(ColumnType type) {
		final BufferAllocator childAllocator = m_rootAllocator.newChildAllocator("ChildAllocator", 0,
				m_rootAllocator.getLimit());
		switch (type) {
		case BOOLEAN:
			return new ArrowBooleanColumnPartitionStore(m_batchSize, childAllocator, m_baseDirectory);
		case DOUBLE:
			return new ArrowDoubleColumnPartitionStore(m_batchSize, childAllocator, m_baseDirectory);
		case STRING:
			return new ArrowStringColumnPartitionStore(m_batchSize, childAllocator, m_baseDirectory);
		default:
			throw new UnsupportedOperationException("not yet implemented");
		}
	}
}
