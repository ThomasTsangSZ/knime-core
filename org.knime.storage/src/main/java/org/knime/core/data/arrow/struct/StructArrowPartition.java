package org.knime.core.data.arrow.struct;

import org.knime.core.data.table.column.Partition;

class StructArrowPartition implements Partition<Partition<?>[]> {

	private Partition<?>[] m_partitions;

	public StructArrowPartition(Partition<?>[] partitions) {
		m_partitions = partitions;
	}

	@Override
	public void close() throws Exception {
		for (int i = 0; i < m_partitions.length; i++) {
			m_partitions[i].close();
		}
	}

	@Override
	public Partition<?>[] get() {
		return m_partitions;
	}

	@Override
	public long getCapacity() {
		// assumption: all partitions have same capacity
		return m_partitions[0].getCapacity();
	}

	@Override
	public void setNumValues(int numValues) {
		for (int i = 0; i < m_partitions.length; i++) {
			m_partitions[i].setNumValues(numValues);
		}
	}

	@Override
	public int getNumValues() {
		return m_partitions[0].getNumValues();
	}

}
