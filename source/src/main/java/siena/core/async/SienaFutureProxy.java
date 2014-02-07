package siena.core.async;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * This is Objectify's ResultProxy. 
 * A dynamic proxy that wraps a SienaFuture<?> value.  For example, if you had a Result<List<String>>, the
 * proxy would implement List<String> and call through to the inner object.
 */
public class SienaFutureProxy<T> implements InvocationHandler
{
	/**
	 * Create a ResultProxy for the given interface.
	 */
	@SuppressWarnings("unchecked")
	public static <S> S create(Class<? super S> interf, SienaFuture<S> sienaFuture) {
		return (S)Proxy.newProxyInstance(sienaFuture.getClass().getClassLoader(), new Class[] { interf }, new SienaFutureProxy<S>(sienaFuture));
	}

	SienaFuture<T> sienaFuture;

	private SienaFutureProxy(SienaFuture<T> sienaFuture) {
		this.sienaFuture = sienaFuture;
	}

	@Override
	public Object invoke(Object obj, Method meth, Object[] params) throws Throwable {
		return meth.invoke(sienaFuture.get(), params);
	}
}
