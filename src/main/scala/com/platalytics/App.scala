package com.platalytics

/**
 * @author ${user.name}
 */
object App {
  
  
  def main(args : Array[String]) {
    var temp = new HSCustomer()
    temp.HighestSpendingCustomer(args)
  }

}
