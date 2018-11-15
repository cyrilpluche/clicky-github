object Timer {
    def getExecutionTime[R] (block: => R): R = {
        val t0 = System.nanoTime()
        val result = block 
        val t1 = System.nanoTime()
        println("Execution time: " + (t1 - t0) + "ns")
        result
    }
}