package org.knime.core.data.vector;

import java.util.concurrent.atomic.AtomicInteger;

// TODO thread safety?
public abstract class AbstractRefManaged implements RefManaged {

	private AtomicInteger m_referenceCounter;

	protected AbstractRefManaged() {
		m_referenceCounter = new AtomicInteger();
	}

	@Override
	public synchronized void release() {
		if (m_referenceCounter.decrementAndGet() == 0) {
			onAllReferencesReleased();
		}
	}

	@Override
	public synchronized void retain() {
		m_referenceCounter.incrementAndGet();
	}

	protected abstract void onAllReferencesReleased();

}
