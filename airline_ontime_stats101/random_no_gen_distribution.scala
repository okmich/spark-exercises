//Random variable generation (probability distribution)
//==============================
// Random data generation is useful for testing of existing algorithms 
// and implementing randomized algorithms, such as random projection.
import org.apache.spark.mllib.random._

// generate 20 random numbers for an exponential distribution with mean of 12.9
val expGen = new ExponentialGenerator(12.9)
val expRandomNumbers = for (i <- (1 to 20)) yield expGen.nextValue

// generate 20 random numbers for a gamma distribution with mean of 8 and scale of 10
val gGen = new GammaGenerator(8, 10)
val gammaRandomNumbers = for (i <- (1 to 20)) yield gGen.nextValue


// generate 20 random numbers for a lognormal distribution with mean of 25 and standard deviation of 7
val lnGen = new LogNormalGenerator(25, 7)
val lnRandomNumbers = for (i <- (1 to 20)) yield lnGen.nextValue

//PoissonGenerator
//StandardNormalGenerator
//UniformGenerator

val uGen = new UniformGenerator()
val uRandomNumbers = for (i <- (1 to 20)) yield uGen.nextValue


//if you want to create a random number generator for a distribution not available in spark,
//you will have to implement the org.apache.spark.mllib.random.RandomDataGenerator trait 
//and implement the method to suit the distribution you seek

