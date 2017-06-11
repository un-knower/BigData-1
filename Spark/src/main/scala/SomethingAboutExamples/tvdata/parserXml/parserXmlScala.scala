package SomethingAboutExamples.tvdata.parserXml

import java.net.URLDecoder

import org.dom4j.{DocumentHelper, Element}

/**
  * Created by HuShiwei on 2016/9/8 0008.
  */
object parserXmlScala {

  def parser(line: String) = {
    val document = DocumentHelper.parseText(line)
    val rootElement: Element = document.getRootElement
    val WICElement: Element = rootElement.element("WIC")
    val stbNum: String = WICElement.attributeValue("stbNum")
    val date: String = WICElement.attributeValue("date")
    println(stbNum)
    println(date)
    val listA = WICElement.elements("A")

    for (element:Element <- listA.asInstanceOf[List[Element]]) {
      val endTime=element.attributeValue("e")
      val startTime=element.attributeValue("s")
      val p=element.attributeValue("p")
      val show=URLDecoder.decode(p,"utf-8")
      val sn=element.attributeValue("sn")
      println(show)

    }

  }

  def main(args: Array[String]) {
    val xmlStr = "<GHApp>\n    <WIC cardNum=\"110012895\" stbNum=\"01051009200143198\" date=\"2012-09-21\" pageWidgetVersion=\"1.0\">\n        <A e=\"01:27:31\" s=\"01:22:31\" n=\"187\" t=\"6\" pi=\"533\" p=\"%E7%89%B9%E6%88%98%E5%85%88%E9%94%8B(13)\" sn=\"广西卫视\" />\n    </WIC>\n</GHApp>"
    parser(xmlStr)
  }

}
