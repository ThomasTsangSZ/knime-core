package org.knime.core.data.store;

// TODO here we can implement caching INDEPENDENT from memory / storage layout.
public class CachedStore<B extends Batch> implements Store<B> {

	private Store<B> m_delegate;

	public CachedStore(Store delegate) {
		m_delegate = delegate;
	}

	@Override
	public void persist(B batch) {
		m_delegate.persist(batch);
	}

	@Override
	public B load(long idx) {
		return m_delegate.load(idx);
	}

	@Override
	public B createNext() {
		return m_delegate.createNext();
	}

	@Override
	public void destroy() {
		m_delegate.destroy();
	}

}
