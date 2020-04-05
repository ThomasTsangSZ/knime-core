package org.knime.core.data.arrow.struct;

import org.knime.core.data.partition.Partition;

class StructArrowPartition implements Partition<Partition<?>[]> {

	private Partition<?>[] m_partitions;
	private long m_index;

	public StructArrowPartition(Partition<?>[] partitions, long index) {
		m_partitions = partitions;
		m_index = index;
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
	public void setNumValuesWritten(int numValues) {
		for (int i = 0; i < m_partitions.length; i++) {
			m_partitions[i].setNumValuesWritten(numValues);
		}
	}

	@Override
	public int getNumValuesWritten() {
		return m_partitions[0].getNumValuesWritten();
	}

	@Override
	public long getIndex() {
		return m_index;
	}

}
