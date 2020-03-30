package org.knime.core.data.store.table.column;

import org.knime.core.data.store.table.row.WritableValueAccess;

public class WritableDoubleColumn implements WritableColumn {

	@Override
	public void close() throws Exception {
		// TODO Auto-generated method stub
	}

	@Override
	public void fwd() {
		// TODO Auto-generated method stub

	}

	@Override
	public WritableValueAccess get() {
		// TODO Auto-generated method stub
		return null;
	}

	class WritableDoubleValueAccess implements WritableValueAccess {

		@Override
		public void setMissing() {

		}
	}
}
