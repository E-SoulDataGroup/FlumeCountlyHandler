/**
 * Created by Administrator on 2017/1/9.
 */
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import java.io.BufferedReader;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.UnsupportedCharsetException;
import java.util.*;
import javax.servlet.http.HttpServletRequest;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.event.JSONEvent;
import org.apache.flume.source.http.HTTPBadRequestException;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.apache.flume.source.http.JSONHandler;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CountlyHandler implements HTTPSourceHandler {

    private static final Logger LOG = LoggerFactory.getLogger(JSONHandler.class);
    private final Type listType = new TypeToken<List<JSONEvent>>() {}.getType();
    private final Gson gson;
    private boolean splitEventsFlag;

    public CountlyHandler() {
        gson = new GsonBuilder().disableHtmlEscaping().create();
    }

    @Override
    public void configure(Context context) {
        splitEventsFlag = context.getBoolean("splitEvents", false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Event> getEvents(HttpServletRequest request) throws Exception {
        BufferedReader reader = request.getReader();
        String charset = request.getCharacterEncoding();
        //UTF-8 is default for JSON. If no charset is specified, UTF-8 is to
        //be assumed.
        if (charset == null) {
            LOG.debug("Charset is null, default charset of UTF-8 will be used.");
            charset = "UTF-8";
        } else if (!(charset.equalsIgnoreCase("utf-8")
                || charset.equalsIgnoreCase("utf-16")
                || charset.equalsIgnoreCase("utf-32"))) {
            LOG.error("Unsupported character set in request {}. "
                    + "JSON handler supports UTF-8, "
                    + "UTF-16 and UTF-32 only.", charset);
            throw new UnsupportedCharsetException("JSON handler supports UTF-8, "
                    + "UTF-16 and UTF-32 only.");
        }

    /*
     * Gson throws Exception if the data is not parseable to JSON.
     * Need not catch it since the source will catch it and return error.
     */
        List<Event> eventList = new ArrayList<Event>(0);
        try {
            String result = countlyToJSON(reader);
            if (splitEventsFlag) {
                result = splitEvents(result);
            }
            LOG.info("[ Message ]:" + result);
            eventList = gson.fromJson(result, listType);
        } catch (JsonSyntaxException ex) {
            throw new HTTPBadRequestException("Request has invalid JSON Syntax.", ex);
        }

        for (Event e : eventList) {
            ((JSONEvent) e).setCharset(charset);
        }
        return getSimpleEvents(eventList);
    }

    private List<Event> getSimpleEvents(List<Event> events) {
        List<Event> newEvents = new ArrayList<Event>(events.size());
        for (Event e:events) {
            newEvents.add(EventBuilder.withBody(e.getBody(), e.getHeaders()));
        }
        return newEvents;
    }

    private String countlyToJSON(BufferedReader reader) {
        String result = "";
        Map<String, String> replaceItems = new HashMap<String, String>();
        replaceItems.put("app_key=", "\"app_key\":\"");
        replaceItems.put("&timestamp=", "\",\"timestamp\":\"");
        replaceItems.put("&hour=", "\",\"hour\":\"");
        replaceItems.put("&dow=", "\",\"dow\":\"");
        replaceItems.put("&events=", "\",\"events\":");
        replaceItems.put("&sdk_version=", ",\"sdk_version\":\"");
        replaceItems.put("&sdk_name=", "\",\"sdk_name\":\"");
        replaceItems.put("&device_id=", "\",\"device_id\":\"");

        try{
            String msg = org.apache.commons.io.IOUtils.toString(reader);
            result = java.net.URLDecoder.decode(msg, "UTF-8");
            if (result.contains("events=")) { // 包含events的数据单独处理，防止内部包含自定义特殊字符
                for (Map.Entry<String, String> entry : replaceItems.entrySet())
                {
                    result = result.replace(entry.getKey(), entry.getValue());
                }
                result = "{" + result + "\"}";
            }
            else { // 不包含events的数据可以统一进行替换
                result = "{\"" + result.replace("=[", "\":[").replace("]&", "],\"")
                        .replace("={", "\":{").replace("}&", "},\"")
                        .replace("=", "\":\"").replace("&", "\",\"").replace("length\":\"", "length=")
                        .replace("index\":\"", "index=").replace("\n", " ") + "\"}";
            }
            result = "[{\"headers\":{},\"body\":\"" + result.replace("\"", "\\\"") + "\"}]";
            //LOG.info("[ Message ]:" + result);
        } catch (IOException ex){}
        return result;
    }

    private String splitEvents(String countlyJSON) {
        if (countlyJSON.contains("events")) {
            List<Event> eventList = gson.fromJson(countlyJSON, listType);
            String a = new String(eventList.get(0).getBody());
            JSONObject objectRaw = new JSONObject(a);
            JSONArray arrayNew = new JSONArray();
            JSONArray events = objectRaw.getJSONArray("events");
            objectRaw.remove("events");
            for (int i = 0; i < events.length(); i++) {
                JSONObject objectNow = new JSONObject(objectRaw, JSONObject.getNames(objectRaw));
                JSONObject eventObjectItem = events.getJSONObject(i);
                Iterator<?> keys = eventObjectItem.keys();
                while( keys.hasNext() ) {
                    String keyRaw = (String)keys.next();
                    String key = "event_" + keyRaw;
                    objectNow.put(key, eventObjectItem.get(keyRaw));
                }
                arrayNew.put(new JSONObject("{\"headers\":{},\"body\":\"" + objectNow.toString().replace("\"", "\\\"") + "\"}"));
            }
            return arrayNew.toString();
        }
        else {
            return countlyJSON;
        }
    }
}
