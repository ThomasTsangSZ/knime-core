package org.knime.core.data.store;

public interface Batch extends AutoCloseable {

	BatchSpec getSpec();

	BatchColumnReadAccess getReadAccessAt(int idx);
	
	BatchColumnWriteAccess getWriteAccessAt(int idx);

}
