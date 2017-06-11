package cn.scala.chapter12;//package cn.scala.chapter15;
//
//import scala.collection.immutable.List;
//
//class MySQLDAOImpl1 implements MySQLDAO1{
//    @Override
//    public boolean delete(String id) {
//        if (MySQLDAO1$class.delete(this, id)) return true;
//        else return false;
//    }
//
//    @Override
//    public boolean add(Object o) {
//        return false;
//    }
//
//    @Override
//    public int update(Object o) {
//        return 0;
//    }
//
//    @Override
//    public List<Object> query(String id) {
//        return null;
//    }
//}
//public class TraitInJava1 {
//    public static void main(String[] args) {
//        MySQLDAO1 mySQLDAO=new MySQLDAOImpl1();
//        mySQLDAO.delete("100");
//    }
//}