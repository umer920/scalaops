package TestScala.SacalaOps

/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    var temp = new HRItems()
    temp.HihghestReturnedItems(args);
  }

}
