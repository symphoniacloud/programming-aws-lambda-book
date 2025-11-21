package book;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class ListMapLambda {
    public List<Integer> handlerList(List<Integer> input) {
        var newList = new ArrayList<Integer>();
        input.forEach(x -> newList.add(100 + x));
        return newList;
    }

    public Map<String, String> handlerMap(Map<String, String> input) {
        var newMap = new HashMap<String, String>();
        input.forEach((k, v) -> newMap.put("New Map -> " + k, v));
        return newMap;
    }

    public Map<String, Map<String, Integer>> handlerNestedCollection(List<Map<String, Integer>> input) {
        var newMap = new HashMap<String, Map<String, Integer>>();
        IntStream.range(0, input.size())
                .forEach(i -> newMap.put("Nested at position " + i, input.get(i)));
        return newMap;
    }
}