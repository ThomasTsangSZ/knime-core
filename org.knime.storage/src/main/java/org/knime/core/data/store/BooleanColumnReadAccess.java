package org.knime.core.data.store;

public interface BooleanColumnReadAccess extends BatchColumnReadAccess {
	
	boolean get();

	@Override
	default BatchColumnType getType() {
		return BatchColumnType.BOOLEAN;
	}
}
