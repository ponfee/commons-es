package uss1.req;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.fastjson.JSON;

/**
 * USS request scroll params
 * 
 * 接口类型为“查询”，是否有导数需求选“是”
 * 
 * <pre>
 * first search params:
 *  {
 *    "app": "appid",
 *    "searchId": 480,
 *    "params": {
 *      "code": "0100009656"
 *    }
 *  }
 * 
 * search reponse: 
 *  {
 *      "requestId": null,
 *      "success": true,
 *      "business": null,
 *      "errorCode": null,
 *      "errorMessage": null,
 *      "params": null,
 *      "date": "2018-09-19 15:23:36",
 *      "version": null,
 *      "obj": {
 *          "hitNum": 20000,
 *          "returnNum": 5000,
 *          "tookTime": 0,
 *          "hits": [
 *              {
 *                  "create_time": "2018-05-21 15:49:05",
 *                  "customer_code": "0100009656",
 *                  "table_name": "pl_get_bill_detail"
 *              }...
 *          ],
 *          "scrollId": "DnF1ZXJ5VGhlbkZldGNoBQAAAAABy7xkFnZjTUtzSS1tUjhxZHFhTHBiT2JMMlEAAAAAAcu8ZRZ2Y01Lc0ktbVI4cWRxYUxwYk9iTDJRAAAAAAHLvGYWdmNNS3NJLW1SOHFkcWFMcGJPYkwyUQAAAAABy7xoFnZjTUtzSS1tUjhxZHFhTHBiT2JMMlEAAAAAAcu8ZxZ2Y01Lc0ktbVI4cWRxYUxwYk9iTDJR"
 *      }
 *  }
 *  
 * second(or after) search params: 
 *  {
 *      "app": "appid",
 *      "searchId": 480,
 *      "params": {
 *          "code": "0100009656",
 *          "scrollId": "DnF1ZXJ5VGhlbkZldGNoBQAAAAABy7xkFnZjTUtzSS1tUjhxZHFhTHBiT2JMMlEAAAAAAcu8ZRZ2Y01Lc0ktbVI4cWRxYUxwYk9iTDJRAAAAAAHLvGYWdmNNS3NJLW1SOHFkcWFMcGJPYkwyUQAAAAABy7xoFnZjTUtzSS1tUjhxZHFhTHBiT2JMMlEAAAAAAcu8ZxZ2Y01Lc0ktbVI4cWRxYUxwYk9iTDJR"
 *      }
 *  }
 * </pre>
 * 
 * @author Ponfee
 */
public class ScrollParams extends BaseParams {

    private static final long serialVersionUID = 1241888462624598827L;

    private String scrollId;

    public ScrollParams(String params, String prevScrollId) {
        super(params);
        this.scrollId = prevScrollId;
    }

    public String getScrollId() {
        return scrollId;
    }

    public void setScrollId(String scrollId) {
        this.scrollId = scrollId;
    }

    @SuppressWarnings("unchecked")
    @Override
    public String buildParams() {
        if (StringUtils.isEmpty(scrollId)) {
            return getParams();
        }
        Map<String, Object> params = JSON.parseObject(getParams(), Map.class);
        params.put("scrollId", scrollId);
        return JSON.toJSONString(params);
    }

}
