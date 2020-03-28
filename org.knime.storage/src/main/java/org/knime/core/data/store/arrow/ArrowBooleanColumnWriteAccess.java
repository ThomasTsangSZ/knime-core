package org.knime.core.data.store.arrow;

import org.knime.core.data.store.BatchColumnType;
import org.knime.core.data.store.BatchColumnWriteAccess;

public class ArrowBooleanColumnWriteAccess implements BatchColumnWriteAccess {

	@Override
	public BatchColumnType getType() {
		return null;
	}

	@Override
	public void fwd() {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean canForward() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setMissing() {
		// TODO Auto-generated method stub

	}

}
