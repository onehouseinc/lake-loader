package scala.math;

/**
 * Test-scope shim: Spark 3.5.3's KryoSerializer.newKryo does a
 * Class.forName("scala.math.Ordering$Reverse") which only exists in Scala 2.13+.
 * Under Scala 2.12 this throws ClassNotFoundException at SparkContext startup.
 * Providing an empty stub here satisfies the reflective lookup at test time.
 * This class is not shaded into the production jar (test-scope only).
 */
public final class Ordering$Reverse {}
