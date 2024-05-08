package flink;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class MyUtil {

    public static <T> List<T> toList(Iterable<T> elements){
        List<T> result = new ArrayList<>();
        for (T element : elements) {
            result.add(element);
        }
        return result;
    }

    public static String printTimeWindow(TimeWindow t){

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

       return "窗口[ " + format.format(new Date(t.getStart())) + "," + new Date(t.getEnd()) + " ) ";

    }

}
