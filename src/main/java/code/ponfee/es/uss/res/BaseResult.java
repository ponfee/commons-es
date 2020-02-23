package code.ponfee.es.uss.res;

import java.beans.Transient;
import java.io.Serializable;
import java.util.Date;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;

import code.ponfee.commons.ws.adapter.MarshalJsonResult;

/**
 * USS base result
 * 
 * @author Ponfee
 */
public class BaseResult implements Serializable, MarshalJsonResult {

    private static final long serialVersionUID = -4846734605399283040L;

    // ---------------------------------head field
    private Boolean success;
    private Date date;
    private String requestId;
    private String business;
    private String errorCode;
    private String errorMessage;
    private String params;
    private String version;

    public BaseResult() {}

    public BaseResult(BaseResult base) {
        if (base != null) {
            this.success = base.success;
            this.date = base.date;
            this.requestId = base.requestId;
            this.business = base.business;
            this.errorCode = base.errorCode;
            this.errorMessage = base.errorMessage;
            this.params = base.params;
            this.version = base.version;
        } else {
            this.success = false;
            this.date = new Date();
        }
    }

    public static BaseResult failure(String errorMsg) {
        return failure(null, errorMsg);
    }

    public static BaseResult failure(String errorCode, String errorMsg) {
        BaseResult result = new BaseResult();
        result.setSuccess(false);
        result.setDate(new Date());
        result.setErrorCode(errorCode);
        result.setErrorMessage(errorMsg);
        return result;
    }

    public Boolean getSuccess() {
        return success;
    }

    public void setSuccess(Boolean success) {
        this.success = success;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public String getBusiness() {
        return business;
    }

    public void setBusiness(String business) {
        this.business = business;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getParams() {
        return params;
    }

    public void setParams(String params) {
        this.params = params;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    // -------------------------------------------------------others
    @Transient
    public boolean isFailure() {
        return success == null || !success;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this, SerializerFeature.DisableCircularReferenceDetect);
    }
}
