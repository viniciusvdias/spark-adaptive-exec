package br.ufmg.cs.systems.util

/**
 * This snippet was borrowed from jorgeortiz85:
 * https://gist.github.com/jorgeortiz85/908035
 *
 * It provides access to private methods and fields.
 */

// Usage:
//   p(instance)('privateMethod)(arg1, arg2, arg3)

class PrivateMethodCaller(x: AnyRef, methodName: String) {
  def apply(_args: Any*): Any = {
    val args = _args.map(_.asInstanceOf[AnyRef])
    def _parents: Stream[Class[_]] = Stream(x.getClass) #::: _parents.map(_.getSuperclass)
    val parents = _parents.takeWhile(_ != null).toList
    val methods = parents.flatMap(_.getDeclaredMethods)
    val method = methods.find(_.getName == methodName).getOrElse(throw new IllegalArgumentException("Method " + methodName + " not found"))
    method.setAccessible(true)
    method.invoke(x, args : _*)
  }
}

class PrivateMethodExposer(x: AnyRef) {
  def apply(method: scala.Symbol): PrivateMethodCaller = new PrivateMethodCaller(x, method.name)
}

object PrivateMethodExposer {
  def p(x: AnyRef): PrivateMethodExposer = new PrivateMethodExposer(x)
}
