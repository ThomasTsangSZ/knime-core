
package org.knime.core.data.inmemory.array;

// TODO composition over inheritance? :-(
abstract class AbstractNativeArrayFactory<A> implements NativeArrayFactory<A> {

	private final int m_minCapacity;

	public AbstractNativeArrayFactory(final int partitionCapacity) {
		m_minCapacity = partitionCapacity;
	}

	@Override
	public A create() {
		return create(m_minCapacity);
	}

	abstract A create(int capacity);

}
