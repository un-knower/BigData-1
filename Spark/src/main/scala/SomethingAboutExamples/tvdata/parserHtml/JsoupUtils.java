package SomethingAboutExamples.tvdata.parserHtml;

import SomethingAboutExamples.tvdata.utils.StringUtil;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;


public class JsoupUtils {

    public static String parser(String str) {
        /**
         * stbNum、date、e、s、p、sn、e-s
         */
        String stbNum = null;
        String date = null;
        String entTime = null;
        String startTime = null;
        String tvshow = null;
        String p = null;
        String sn = null;
        long spendTime = 0;
//		String str = "<GHApp><WIC cardNum='1370083461' stbNum='03091011120286570' date='2012-09-24' pageWidgetVersion='1.0'><A e='01:42:09' s='01:37:09' n='104' t='1' pi='909' p='%E9%BB%84%E9%A3%9E%E9%B8%BF%E4%B9%8B%E9%93%81%E9%B8%A1%E6%96%97%E8%9C%88%E8%9A%A3' sn='BTV影视' /></WIC></GHApp>";
        Document doc1 = Jsoup.parse(str);
        if (doc1 != null) {
            Element wic = doc1.select("WIC").first();
            if (wic != null) {
                stbNum = wic.attr("stbNum");
                date = wic.attr("date");
            }
            Element a = doc1.select("A").first();
            if (a != null) {
                entTime = a.attr("e");
                startTime = a.attr("s");
                spendTime = StringUtil.spendTime(startTime, entTime);
                p = a.attr("p");
                try {
                    tvshow = URLDecoder.decode(p, "utf-8");
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
                if (tvshow.contains("(")) {
                    tvshow = tvshow.substring(0, tvshow.indexOf("("));
                }
                sn = a.attr("sn").trim();
            }
        }
        return stbNum + "@" + date + "@" + sn + "@" + tvshow + "@" + startTime + "@" + entTime + "@" + spendTime;

    }
    public static void main(String[] args) {
        String xmlstr = "<GHApp><WIC cardNum=\"1370602625\" stbNum=\"03111108020148141\" date=\"2012-09-21\" pageWidgetVersion=\"1.0\"><A e=\"01:25:04\" s=\"23:21:00\" n=\"53\" t=\"11\" pi=\"873\" p=\"娱乐档案\" sn=\"故事广播\" /><A e=\"01:25:19\" s=\"23:21:00\" n=\"53\" t=\"11\" pi=\"873\" p=\"娱乐档案\" sn=\"故事广播\" /><A e=\"01:25:34\" s=\"23:21:00\" n=\"53\" t=\"11\" pi=\"873\" p=\"娱乐档案\" sn=\"故事广播\" /><A e=\"01:25:49\" s=\"23:21:00\" n=\"53\" t=\"11\" pi=\"873\" p=\"娱乐档案\" sn=\"故事广播\" /><A e=\"01:26:04\" s=\"23:21:00\" n=\"53\" t=\"11\" pi=\"873\" p=\"娱乐档案\" sn=\"故事广播\" /><A e=\"01:26:19\" s=\"23:21:00\" n=\"53\" t=\"11\" pi=\"873\" p=\"娱乐档案\" sn=\"故事广播\" /><A e=\"01:26:34\" s=\"23:21:00\" n=\"53\" t=\"11\" pi=\"873\" p=\"娱乐档案\" sn=\"故事广播\" /><A e=\"01:26:49\" s=\"23:21:00\" n=\"53\" t=\"11\" pi=\"873\" p=\"娱乐档案\" sn=\"故事广播\" /><A e=\"01:27:04\" s=\"23:21:00\" n=\"53\" t=\"11\" pi=\"873\" p=\"娱乐档案\" sn=\"故事广播\" /><A e=\"01:27:19\" s=\"23:21:00\" n=\"53\" t=\"11\" pi=\"873\" p=\"娱乐档案\" sn=\"故事广播\" /><A e=\"01:27:34\" s=\"23:21:00\" n=\"53\" t=\"11\" pi=\"873\" p=\"娱乐档案\" sn=\"故事广播\" /><A e=\"01:27:49\" s=\"23:21:00\" n=\"53\" t=\"11\" pi=\"873\" p=\"娱乐档案\" sn=\"故事广播\" /><A e=\"01:28:04\" s=\"23:21:00\" n=\"53\" t=\"11\" pi=\"873\" p=\"娱乐档案\" sn=\"故事广播\" /><A e=\"01:28:19\" s=\"23:21:00\" n=\"53\" t=\"11\" pi=\"873\" p=\"娱乐档案\" sn=\"故事广播\" /><A e=\"01:28:34\" s=\"23:21:00\" n=\"53\" t=\"11\" pi=\"873\" p=\"娱乐档案\" sn=\"故事广播\" /><A e=\"01:28:49\" s=\"23:21:00\" n=\"53\" t=\"11\" pi=\"873\" p=\"娱乐档案\" sn=\"故事广播\" /><A e=\"01:29:04\" s=\"23:21:00\" n=\"53\" t=\"11\" pi=\"873\" p=\"娱乐档案\" sn=\"故事广播\" /><A e=\"01:29:19\" s=\"23:21:00\" n=\"53\" t=\"11\" pi=\"873\" p=\"娱乐档案\" sn=\"故事广播\" /><A e=\"01:29:34\" s=\"23:21:00\" n=\"53\" t=\"11\" pi=\"873\" p=\"娱乐档案\" sn=\"故事广播\" /><A e=\"01:29:49\" s=\"23:21:00\" n=\"53\" t=\"11\" pi=\"873\" p=\"娱乐档案\" sn=\"故事广播\" /></WIC></GHApp>";
        final String parser = parser(xmlstr);
        System.out.println(parser);

    }
}
