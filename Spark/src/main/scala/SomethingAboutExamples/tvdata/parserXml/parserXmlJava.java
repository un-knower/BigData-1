package SomethingAboutExamples.tvdata.parserXml;

import SomethingAboutExamples.tvdata.utils.StringUtil;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.List;

/**
 * Created by HuShiwei on 2016/9/8 0008.
 */
public class parserXmlJava {
    public static String parser(String line) {
        final StringBuilder tmp = new StringBuilder();
        String stbNum = null;
        String date = null;
        String entTime = null;
        String startTime = null;
        String tvshow = null;
        String p = null;
        String sn = null;
        long spendTime = 0;
        try {
            final Document document = DocumentHelper.parseText(line);
            if (document != null) {
                final Element rootElement = document.getRootElement();
                final Element wic = rootElement.element("WIC");
                if (wic != null) {
                    stbNum = wic.attributeValue("stbNum");
                    date = wic.attributeValue("date");
                }
                final List<Element> Aelements = wic.elements("A");
                final Element element = Aelements.get(0);

                entTime = element.attributeValue("e");
                startTime = element.attributeValue("s");
                spendTime = StringUtil.spendTime(startTime, entTime);
                p = element.attributeValue("p").trim();
                tvshow = URLDecoder.decode(p, "utf-8");
                if (tvshow.contains("(")) {
                    tvshow = tvshow.substring(0, tvshow.indexOf("("));
                }
                sn = element.attributeValue("sn").trim();
            }
        } catch (DocumentException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }


        return stbNum + "@" + date + "@" + sn + "@" + tvshow + "@" + startTime + "@" + entTime + "@" + spendTime;
    }

    public static void main(String[] args) {
        String xmlstr = "<GHApp><WIC cardNum=\"1370602625\" stbNum=\"03111108020148141\" date=\"2012-09-21\" pageWidgetVersion=\"1.0\"><A e=\"01:25:04\" s=\"23:21:00\" n=\"53\" t=\"11\" pi=\"873\" p=\"娱乐档案\" sn=\"故事广播\" /><A e=\"01:25:19\" s=\"23:21:00\" n=\"53\" t=\"11\" pi=\"873\" p=\"娱乐档案\" sn=\"故事广播\" /><A e=\"01:25:34\" s=\"23:21:00\" n=\"53\" t=\"11\" pi=\"873\" p=\"娱乐档案\" sn=\"故事广播\" /><A e=\"01:25:49\" s=\"23:21:00\" n=\"53\" t=\"11\" pi=\"873\" p=\"娱乐档案\" sn=\"故事广播\" /><A e=\"01:26:04\" s=\"23:21:00\" n=\"53\" t=\"11\" pi=\"873\" p=\"娱乐档案\" sn=\"故事广播\" /><A e=\"01:26:19\" s=\"23:21:00\" n=\"53\" t=\"11\" pi=\"873\" p=\"娱乐档案\" sn=\"故事广播\" /><A e=\"01:26:34\" s=\"23:21:00\" n=\"53\" t=\"11\" pi=\"873\" p=\"娱乐档案\" sn=\"故事广播\" /><A e=\"01:26:49\" s=\"23:21:00\" n=\"53\" t=\"11\" pi=\"873\" p=\"娱乐档案\" sn=\"故事广播\" /><A e=\"01:27:04\" s=\"23:21:00\" n=\"53\" t=\"11\" pi=\"873\" p=\"娱乐档案\" sn=\"故事广播\" /><A e=\"01:27:19\" s=\"23:21:00\" n=\"53\" t=\"11\" pi=\"873\" p=\"娱乐档案\" sn=\"故事广播\" /><A e=\"01:27:34\" s=\"23:21:00\" n=\"53\" t=\"11\" pi=\"873\" p=\"娱乐档案\" sn=\"故事广播\" /><A e=\"01:27:49\" s=\"23:21:00\" n=\"53\" t=\"11\" pi=\"873\" p=\"娱乐档案\" sn=\"故事广播\" /><A e=\"01:28:04\" s=\"23:21:00\" n=\"53\" t=\"11\" pi=\"873\" p=\"娱乐档案\" sn=\"故事广播\" /><A e=\"01:28:19\" s=\"23:21:00\" n=\"53\" t=\"11\" pi=\"873\" p=\"娱乐档案\" sn=\"故事广播\" /><A e=\"01:28:34\" s=\"23:21:00\" n=\"53\" t=\"11\" pi=\"873\" p=\"娱乐档案\" sn=\"故事广播\" /><A e=\"01:28:49\" s=\"23:21:00\" n=\"53\" t=\"11\" pi=\"873\" p=\"娱乐档案\" sn=\"故事广播\" /><A e=\"01:29:04\" s=\"23:21:00\" n=\"53\" t=\"11\" pi=\"873\" p=\"娱乐档案\" sn=\"故事广播\" /><A e=\"01:29:19\" s=\"23:21:00\" n=\"53\" t=\"11\" pi=\"873\" p=\"娱乐档案\" sn=\"故事广播\" /><A e=\"01:29:34\" s=\"23:21:00\" n=\"53\" t=\"11\" pi=\"873\" p=\"娱乐档案\" sn=\"故事广播\" /><A e=\"01:29:49\" s=\"23:21:00\" n=\"53\" t=\"11\" pi=\"873\" p=\"娱乐档案\" sn=\"故事广播\" /></WIC></GHApp>";
        final String parser = parser(xmlstr);
        System.out.println(parser);

    }
}
