// EXERCISE-1
// Write some code that takes the value of pi, doubles it, and then prints it within a string with
// three decimal places of precision to the right.
val piDouble: Float= 3.14159265f*2
println(f"double of pi is $piDouble%.3f")


// EXERCISE-2
// Write some code that prints out the first 10 values of the Fibonacci sequence.
// This is the sequence where every number is the sum of the two numbers before it.
// So, the result should be 0, 1, 1, 2, 3, 5, 8, 13, 21, 34

def fib(n:Int): Int ={
  if(n<=1)
    return n
  else
    return fib(n-1)+fib(n-2)
}
for (i <- 0 until 10){
  println(fib(i))
}

// EXERCISE-4
// Create a list of the numbers 1-20; your job is to print out numbers that are evenly divisible by three. (Scala's
// modula operator, like other languages, is %, which gives you the remainder after division. For example, 9 % 3 = 0
// because 9 is evenly divisible by 3.) Do this first by iterating through all the items in the list and testing each
// one as you go. Then, do it again by using a filter function on the list instead.


val numList=List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,17,18, 19 ,20)

// Using for each
for (num <- numList){
  if (num % 3 == 0)
    println(num)
}

//Using List filter function
val divisibleBy3 = numList.filter((x: Int) => x % 3 ==0)
