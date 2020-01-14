package code.ponfee.es.uss;

import code.ponfee.commons.json.Jsons;

public class SearcherConstants {

    public static Searcher client() {
        return new Searcher("", "");
    }

    public static void console(Object obj) {
        System.out.println(Jsons.toJson(obj));
    }
}
