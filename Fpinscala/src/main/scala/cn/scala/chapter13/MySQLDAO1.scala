package cn.scala.chapter13

//全部是抽象成员，与java的interface等同
trait MySQLDAO1{
  def delete(id:String):Boolean={
    println("delete")
    false
  }
  def add(o:Any):Boolean
  def update(o:Any):Int
  def query(id:String):List[Any]
}

class MySQLDAOImpl1 extends MySQLDAO1{
  override def delete(id: String): Boolean = super.delete(id)
  def add(o:Any):Boolean={println("add");true}
  def update(o:Any):Int={println("update");1}
  def query(id:String):List[Any]={println("query");null}
}