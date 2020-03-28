package org.knime.core.data.store;

public interface BatchSpec {

	int getNumColumns();

	BatchColumnType getTypeAt(int i);

}
